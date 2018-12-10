package org.corfudb.infrastructure.management;


/**
 * This is a factory for creating concrete {@link ClusterRecommendationEngine}s based on
 * the requested {@link ClusterRecommendationStrategy}s.
 *
 * Created by Sam Behnam on 10/19/18.
 */
public class ClusterRecommendationEngineFactory {

    private ClusterRecommendationEngineFactory() {
        // To prevent instantiation of the factory class.
    }

    /**
     * Create an instance of {@link ClusterRecommendationEngine} based on the provided
     * {@link ClusterRecommendationStrategy}.
     *
     * @param strategy a {@link ClusterRecommendationStrategy} representing desired
     *               algorithm to be used for determining failure and healing status of Corfu
     *               servers.
     * @return a concrete instance of {@link ClusterRecommendationEngine} specific to the
     * provided strategy.
     */
    public static ClusterRecommendationEngine createForStrategy(ClusterRecommendationStrategy strategy) {
        switch (strategy) {
            case FULLY_CONNECTED_CLUSTER:
                return new FullyConnectedClusterRecommendationEngine();
            case CENTRALIZED_CLUSTER:
                throw new UnsupportedOperationException(strategy.name());
            default:
                throw new UnsupportedOperationException("Unknown ClusterRecommendationStrategy");

        }
    }
}
