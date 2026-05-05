/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.plan.nodes.exec.serde;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.ObjectCodec;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.BeanDescription;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationConfig;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonDeserializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.BeanDeserializerBase;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.BeanDeserializerModifier;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.DelegatingDeserializer;

import java.io.IOException;

/**
 * Repopulates {@link ExecNodeBase#context} after Jackson 2.20+ polymorphic deserialization
 * (FLINK-38280).
 *
 * <p>Jackson 2.20 regressed the interaction between {@code @JsonTypeInfo(visible=true,
 * include=EXISTING_PROPERTY, property="type")} and a same-named
 * {@code @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context} constructor parameter on every
 * {@link ExecNodeBase} subclass. After polymorphic resolution consumes the {@code "type"} token,
 * Jackson 2.20 no longer surfaces it to the bean deserializer, so the {@code context} parameter
 * ends up null. {@link ExecNodeBase} tolerates this via {@link
 * ExecNodeBase#rehydrateContext(ExecNodeContext)}; this modifier wraps the standard {@link
 * BeanDeserializerBase} for every {@link ExecNodeBase} subclass and calls {@code rehydrateContext}
 * after the bean is constructed, using the discriminator string we re-extract from the buffered
 * JSON tree.
 *
 * <p>The wrapper buffers the input object as a {@link JsonNode}, reads the {@code "type"} field,
 * then re-feeds the same JSON tree to the underlying {@link BeanDeserializerBase}. This is the
 * simplest reliable way to capture the discriminator: {@code DeserializationContext} does not
 * expose a stable hook to retrieve the just-resolved type id under the 2.20 code path.
 */
@Internal
final class ExecNodeBeanDeserializerModifier extends BeanDeserializerModifier {

    @Override
    public JsonDeserializer<?> modifyDeserializer(
            DeserializationConfig config,
            BeanDescription beanDesc,
            JsonDeserializer<?> deserializer) {
        if (!ExecNodeBase.class.isAssignableFrom(beanDesc.getBeanClass())) {
            return deserializer;
        }
        if (!(deserializer instanceof BeanDeserializerBase)) {
            return deserializer;
        }
        return new ExecNodeContextRehydratingDeserializer((BeanDeserializerBase) deserializer);
    }

    private static final class ExecNodeContextRehydratingDeserializer
            extends DelegatingDeserializer {

        private static final long serialVersionUID = 1L;

        ExecNodeContextRehydratingDeserializer(BeanDeserializerBase delegate) {
            super(delegate);
        }

        @Override
        protected JsonDeserializer<?> newDelegatingInstance(JsonDeserializer<?> newDelegatee) {
            return new ExecNodeContextRehydratingDeserializer((BeanDeserializerBase) newDelegatee);
        }

        @Override
        public Object deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            return readAndRehydrate(p, ctxt);
        }

        @Override
        public Object deserializeWithType(
                JsonParser p,
                DeserializationContext ctxt,
                org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.jsontype
                                .TypeDeserializer
                        typeDeserializer)
                throws IOException {
            // Polymorphic dispatch on ExecNode subtypes routes through here. We re-buffer the
            // object so the embedded "type" discriminator is captured for rehydration; the
            // typeDeserializer's resolution result is implicitly preserved because the delegated
            // BeanDeserializer is already the concrete subtype's deserializer.
            return readAndRehydrate(p, ctxt);
        }

        private Object readAndRehydrate(JsonParser p, DeserializationContext ctxt)
                throws IOException {
            ObjectCodec codec = p.getCodec();
            JsonNode tree = (codec == null) ? p.readValueAsTree() : codec.readTree(p);
            String typeToken = null;
            if (tree != null && tree.isObject() && tree.has(ExecNode.FIELD_NAME_TYPE)) {
                typeToken = tree.get(ExecNode.FIELD_NAME_TYPE).asText();
            }
            JsonParser branched = tree.traverse(codec);
            branched.nextToken();
            Object value = ((BeanDeserializerBase) getDelegatee()).deserialize(branched, ctxt);
            if (value instanceof ExecNodeBase && typeToken != null) {
                ((ExecNodeBase<?>) value).rehydrateContext(new ExecNodeContext(typeToken));
            }
            return value;
        }
    }
}
