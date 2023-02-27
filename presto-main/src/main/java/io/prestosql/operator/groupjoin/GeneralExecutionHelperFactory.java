/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.operator.groupjoin;

import com.google.common.util.concurrent.ListeningExecutorService;
import io.prestosql.sql.analyzer.FeaturesConfig;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;

public class GeneralExecutionHelperFactory
        implements ExecutionHelperFactory
{
    private final ListeningExecutorService executor;

    @Inject
    public GeneralExecutionHelperFactory(FeaturesConfig featuresConfig)
    {
        executor = listeningDecorator(newFixedThreadPool(
                requireNonNull(featuresConfig, "featuresConfig is null").getGroupJoinThreads(),
                daemonThreadsNamed("group-join-executor-%s")));
    }

    @Override
    public ExecutionHelper create()
    {
        return new GeneralExecutionHelper(executor);
    }

    @PreDestroy
    public void destroy()
    {
        executor.shutdownNow();
    }
}
