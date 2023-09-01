package org.apache.flink.runtime.state;

import javax.annotation.Nullable;

public interface RestoredStateTransformer<T> {
    @Nullable
    T filterOrTransform(@Nullable T value);

    interface RestoredStateTransformerFactory<T> {
        RestoredStateTransformerFactory<?> NO_TRANSFORM = createNoTransform();

        static <T> RestoredStateTransformerFactory<T> noTransform() {
            return (RestoredStateTransformerFactory<T>) NO_TRANSFORM;
        }

        static <T> RestoredStateTransformerFactory<T> createNoTransform() {
            return restoredMetaInfo -> new RestoredStateTransformer<T>() {
                @Nullable
                @Override
                public T filterOrTransform(@Nullable T value) {
                    return value;
                }
            };
        }

        RestoredStateTransformer<T> createRestoredStateTransformer(
                RegisteredKeyValueStateBackendMetaInfo<?, ?> restoredMetaInfo);
    }


}
