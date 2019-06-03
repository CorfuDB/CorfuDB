package org.corfudb.infrastructure.log;

import org.corfudb.runtime.exceptions.unrecoverable.UnrecoverableCorfuError;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

/**
 * The interface of stream log compaction policy, which decides
 * which log segments will be selected for compaction.
 *
 * Created by WenbinZhu on 5/22/19.
 */
public interface CompactionPolicy {

    /**
     * Returns a list of segments selected for compaction based on
     * the compaction policy implementation.
     *
     * @param compactibleSegments all unprotected segments that can be selected for compaction.
     * @return a list of segments selected for compaction.
     */
    List<StreamLogSegment> getSegmentsToCompact(List<StreamLogSegment> compactibleSegments);

    static CompactionPolicy getPolicy(StreamLogParams params) {
        try {
            Package policyPackagePath = CompactionPolicy.class.getPackage();
            String policyClassName = policyPackagePath.getName() + "." + params.compactionPolicyName;
            Class<?> policyClass = Class.forName(policyClassName);
            Constructor<?> ctor = policyClass.getDeclaredConstructor(StreamLogParams.class);
            return (CompactionPolicy) ctor.newInstance(params);
        } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException
                | IllegalAccessException | InvocationTargetException e) {
            throw new UnrecoverableCorfuError(e);
        }
    }
}
