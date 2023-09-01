package org.apache.flink.runtime.state.ttl;

import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.RestoredStateTransformer;

import javax.annotation.Nullable;

public class TtlStateRestoreTransformer<T> implements RestoredStateTransformer<TtlValue<T>> {
    private final TtlTimeProvider ttlTimeProvider;
    private long oldTtlInMilliseconds;

    public TtlStateRestoreTransformer(TtlTimeProvider ttlTimeProvider, RegisteredKeyValueStateBackendMetaInfo<?, ?> oldMetaInfo) {
        this.ttlTimeProvider = ttlTimeProvider;
        this.oldTtlInMilliseconds = oldMetaInfo.getStateTtlTime().get().toMilliseconds();
    }

    @Nullable
    @Override
    public TtlValue<T> filterOrTransform(@Nullable TtlValue<T> value) {
        if (value != null && TtlUtils.expired(value.getLastAccessTimestamp(), oldTtlInMilliseconds, ttlTimeProvider)) {
            return null;
        }
        return value;
    }


    public static class Factory<TTLV> implements RestoredStateTransformerFactory<TtlValue<TTLV>> {
        private final TtlTimeProvider ttlTimeProvider;

        public Factory(TtlTimeProvider ttlTimeProvider) {
            this.ttlTimeProvider = ttlTimeProvider;
        }

        @Override
        public RestoredStateTransformer<TtlValue<TTLV>> createRestoredStateTransformer(
                RegisteredKeyValueStateBackendMetaInfo<?, ?> restoredMetaInfo) {
            return new TtlStateRestoreTransformer<>(ttlTimeProvider, restoredMetaInfo);
        }
    }
}
