package org.corfudb.infrastructure.management;


/**
 * This is a factory for creating concrete {@link ClusterAdvisor}s based on
 * the requested {@link ClusterType}s.
 *
 * Created by Sam Behnam on 10/19/18.
 */
public class ClusterRecommendationEngineFactory {

    private ClusterRecommendationEngineFactory() {
        // To prevent instantiation of the factory class.
    }

    /**
     * Create an instance of {@link ClusterAdvisor} based on the provided
     * {@link ClusterType}.
     *
     * @param strategy a {@link ClusterType} representing desired
     *               algorithm to be used for determining failure and healing status of Corfu
     *               servers.
     * @return a concrete instance of {@link ClusterAdvisor} specific to the
     * provided strategy.
     */
    public static ClusterAdvisor createForStrategy(ClusterType strategy) {
        switch (strategy) {
            case COMPLETE_GRAPH:
                return new CompleteGraphAdvisor();
            case STAR_GRAPH:
                throw new UnsupportedOperationException(strategy.name());
            default:
                throw new UnsupportedOperationException("Unknown ClusterType");

        }
    }
}
