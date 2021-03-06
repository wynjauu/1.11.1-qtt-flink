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

package org.apache.flink.yarn;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.util.TestLogger;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourcePBImpl;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link WorkerSpecContainerResourceAdapter}.
 */
public class WorkerSpecContainerResourceAdapterTest extends TestLogger {

	@Test
	public void testMatchVcores() {
		final WorkerSpecContainerResourceAdapter.MatchingStrategy strategy =
			WorkerSpecContainerResourceAdapter.MatchingStrategy.MATCH_VCORE;
		final int minMemMB = 100;
		final int minVcore = 10;
		final int unitMemMB = 50;
		final int unitVcore = 5;
		final WorkerSpecContainerResourceAdapter adapter =
			new WorkerSpecContainerResourceAdapter(
				getConfigProcessSpecEqualsWorkerSpec(),
				minMemMB,
				minVcore,
				Integer.MAX_VALUE,
				Integer.MAX_VALUE,
				unitMemMB,
				unitVcore,
				Collections.emptyMap());

		// mem < minMem, vcore < minVcore, should be normalized to [minMem, minVcore]
		final WorkerResourceSpec workerSpec1 = new WorkerResourceSpec.Builder()
			.setCpuCores(8.0)
			.setTaskHeapMemoryMB(20)
			.setTaskOffHeapMemoryMB(20)
			.setNetworkMemoryMB(20)
			.setManagedMemoryMB(20)
			.build();
		// mem = minMem, mem % unitMem = 0, vcore = minVcore, vcore % unitVcore = 0, should not be changed
		final WorkerResourceSpec workerSpec2 = new WorkerResourceSpec.Builder()
			.setCpuCores(10.0)
			.setTaskHeapMemoryMB(25)
			.setTaskOffHeapMemoryMB(25)
			.setNetworkMemoryMB(25)
			.setManagedMemoryMB(25)
			.build();
		// mem > minMem, mem % unitMem != 0, vcore < minVcore, should be normalized to [n * unitMem, minVcore]
		final WorkerResourceSpec workerSpec3 = new WorkerResourceSpec.Builder()
			.setCpuCores(8.0)
			.setTaskHeapMemoryMB(30)
			.setTaskOffHeapMemoryMB(30)
			.setNetworkMemoryMB(30)
			.setManagedMemoryMB(30)
			.build();
		// mem < minMem, vcore > minVcore, vcore % unitVcore != 0, should be normalized to [minMem, n * unitVcore]
		final WorkerResourceSpec workerSpec4 = new WorkerResourceSpec.Builder()
			.setCpuCores(12.0)
			.setTaskHeapMemoryMB(20)
			.setTaskOffHeapMemoryMB(20)
			.setNetworkMemoryMB(20)
			.setManagedMemoryMB(20)
			.build();

		final Resource containerResource1 = Resource.newInstance(100, 10);
		final Resource containerResource2 = Resource.newInstance(150, 10);
		final Resource containerResource3 = Resource.newInstance(100, 15);

		assertThat(adapter.getWorkerSpecs(containerResource1, strategy), empty());
		assertThat(adapter.getWorkerSpecs(containerResource2, strategy), empty());

		assertThat(adapter.tryComputeContainerResource(workerSpec1).get(), is(containerResource1));
		assertThat(adapter.tryComputeContainerResource(workerSpec2).get(), is(containerResource1));
		assertThat(adapter.tryComputeContainerResource(workerSpec3).get(), is(containerResource2));
		assertThat(adapter.tryComputeContainerResource(workerSpec4).get(), is(containerResource3));

		assertThat(adapter.getWorkerSpecs(containerResource1, strategy), containsInAnyOrder(workerSpec1, workerSpec2));
		assertThat(adapter.getWorkerSpecs(containerResource2, strategy), contains(workerSpec3));
		assertThat(adapter.getWorkerSpecs(containerResource3, strategy), contains(workerSpec4));
	}

	@Test
	public void testIgnoreVcores() {
		final WorkerSpecContainerResourceAdapter.MatchingStrategy strategy =
			WorkerSpecContainerResourceAdapter.MatchingStrategy.IGNORE_VCORE;
		final int minMemMB = 100;
		final int minVcore = 1;
		final int unitMemMB = 50;
		final int unitVcore = 1;
		final WorkerSpecContainerResourceAdapter adapter =
			new WorkerSpecContainerResourceAdapter(
				getConfigProcessSpecEqualsWorkerSpec(),
				minMemMB,
				minVcore,
				Integer.MAX_VALUE,
				Integer.MAX_VALUE,
				unitMemMB,
				unitVcore,
				Collections.emptyMap());

		// mem < minMem, should be normalized to [minMem, vcore], equivalent to [minMem, 1]
		final WorkerResourceSpec workerSpec1 = new WorkerResourceSpec.Builder()
			.setCpuCores(5.0)
			.setTaskHeapMemoryMB(10)
			.setTaskOffHeapMemoryMB(10)
			.setNetworkMemoryMB(10)
			.setManagedMemoryMB(10)
			.build();

		// mem < minMem, should be normalized to [minMem, vcore], equivalent to [minMem, 1]
		final WorkerResourceSpec workerSpec2 = new WorkerResourceSpec.Builder()
			.setCpuCores(10.0)
			.setTaskHeapMemoryMB(10)
			.setTaskOffHeapMemoryMB(10)
			.setNetworkMemoryMB(10)
			.setManagedMemoryMB(10)
			.build();

		// mem = minMem, mem % unitMem = 0, should not be changed, equivalent to [mem, 1]
		final WorkerResourceSpec workerSpec3 = new WorkerResourceSpec.Builder()
			.setCpuCores(5.0)
			.setTaskHeapMemoryMB(25)
			.setTaskOffHeapMemoryMB(25)
			.setNetworkMemoryMB(25)
			.setManagedMemoryMB(25)
			.build();

		// mem > minMem, mem % unitMem != 0, should be normalized to [n * unitMem, vcore], equivalent to [n * unitMem, 1]
		final WorkerResourceSpec workerSpec4 = new WorkerResourceSpec.Builder()
			.setCpuCores(5.0)
			.setTaskHeapMemoryMB(30)
			.setTaskOffHeapMemoryMB(30)
			.setNetworkMemoryMB(30)
			.setManagedMemoryMB(30)
			.build();

		final Resource containerResource1 = Resource.newInstance(100, 5);
		final Resource containerResource2 = Resource.newInstance(100, 10);
		final Resource containerResource3 = Resource.newInstance(150, 5);

		final Resource containerResource4 = Resource.newInstance(100, 1);
		final Resource containerResource5 = Resource.newInstance(150, 1);

		assertThat(adapter.tryComputeContainerResource(workerSpec1).get(), is(containerResource1));
		assertThat(adapter.tryComputeContainerResource(workerSpec2).get(), is(containerResource2));
		assertThat(adapter.tryComputeContainerResource(workerSpec3).get(), is(containerResource1));
		assertThat(adapter.tryComputeContainerResource(workerSpec4).get(), is(containerResource3));

		assertThat(adapter.getEquivalentContainerResource(containerResource4, strategy), containsInAnyOrder(containerResource1, containerResource2));
		assertThat(adapter.getEquivalentContainerResource(containerResource5, strategy), contains(containerResource3));

		assertThat(adapter.getWorkerSpecs(containerResource4, strategy), containsInAnyOrder(workerSpec1, workerSpec2, workerSpec3));
		assertThat(adapter.getWorkerSpecs(containerResource5, strategy), contains(workerSpec4));
	}

	@Test
	public void testMaxLimit() {
		final int minMemMB = 100;
		final int minVcore = 1;
		final int maxMemMB = 1000;
		final int maxVcore = 10;
		final int unitMemMB = 100;
		final int unitVcore = 1;
		final WorkerSpecContainerResourceAdapter adapter =
			new WorkerSpecContainerResourceAdapter(
				getConfigProcessSpecEqualsWorkerSpec(),
				minMemMB,
				minVcore,
				maxMemMB,
				maxVcore,
				unitMemMB,
				unitVcore,
				Collections.emptyMap());

		final WorkerResourceSpec workerSpec1 = new WorkerResourceSpec.Builder()
			.setCpuCores(5.0)
			.setTaskHeapMemoryMB(300)
			.setTaskOffHeapMemoryMB(300)
			.setNetworkMemoryMB(300)
			.setManagedMemoryMB(300)
			.build();
		final WorkerResourceSpec workerSpec2 = new WorkerResourceSpec.Builder()
			.setCpuCores(15.0)
			.setTaskHeapMemoryMB(10)
			.setTaskOffHeapMemoryMB(10)
			.setNetworkMemoryMB(10)
			.setManagedMemoryMB(10)
			.build();

		assertFalse(adapter.tryComputeContainerResource(workerSpec1).isPresent());
		assertFalse(adapter.tryComputeContainerResource(workerSpec2).isPresent());
	}

	@Test
	public void testMatchResourceWithDifferentImplementation() {
		final WorkerSpecContainerResourceAdapter.MatchingStrategy strategy =
			WorkerSpecContainerResourceAdapter.MatchingStrategy.IGNORE_VCORE;
		final int minMemMB = 1;
		final int minVcore = 1;
		final int unitMemMB = 1;
		final int unitVcore = 1;

		final WorkerSpecContainerResourceAdapter adapter =
			new WorkerSpecContainerResourceAdapter(
				getConfigProcessSpecEqualsWorkerSpec(),
				minMemMB,
				minVcore,
				Integer.MAX_VALUE,
				Integer.MAX_VALUE,
				unitMemMB,
				unitVcore,
				Collections.emptyMap());

		final WorkerResourceSpec workerSpec = new WorkerResourceSpec.Builder()
			.setCpuCores(1.0)
			.setTaskHeapMemoryMB(100)
			.setTaskOffHeapMemoryMB(200)
			.setNetworkMemoryMB(300)
			.setManagedMemoryMB(400)
			.build();

		Optional<Resource> resourceOpt = adapter.tryComputeContainerResource(workerSpec);
		assertTrue(resourceOpt.isPresent());
		Resource resourceImpl1 = resourceOpt.get();

		Resource resourceImpl2 = new TestingResourceImpl(
			resourceImpl1.getMemory(),
			resourceImpl1.getVirtualCores() + 1);

		assertThat(adapter.getEquivalentContainerResource(resourceImpl2, strategy), contains(resourceImpl1));
		assertThat(adapter.getWorkerSpecs(resourceImpl2, strategy), contains(workerSpec));
	}

	@Test
	public void testMatchInternalContainerResourceIgnoresZeroValueExternalResources() {
		final Map<String, Long> externalResources1 = new HashMap<>();
		final Map<String, Long> externalResources2 = new HashMap<>();

		externalResources1.put("foo", 0L);
		externalResources1.put("bar", 1L);
		externalResources2.put("zoo", 0L);
		externalResources2.put("bar", 1L);

		final WorkerSpecContainerResourceAdapter.InternalContainerResource internalContainerResource1 =
			new WorkerSpecContainerResourceAdapter.InternalContainerResource(1024, 1, externalResources1);
		final WorkerSpecContainerResourceAdapter.InternalContainerResource internalContainerResource2 =
			new WorkerSpecContainerResourceAdapter.InternalContainerResource(1024, 1, externalResources2);

		assertEquals(internalContainerResource1, internalContainerResource2);
	}

	private Configuration getConfigProcessSpecEqualsWorkerSpec() {
		final Configuration config = new Configuration();
		config.set(TaskManagerOptions.FRAMEWORK_HEAP_MEMORY, MemorySize.ZERO);
		config.set(TaskManagerOptions.FRAMEWORK_OFF_HEAP_MEMORY, MemorySize.ZERO);
		config.set(TaskManagerOptions.JVM_METASPACE, MemorySize.ZERO);
		config.set(TaskManagerOptions.JVM_OVERHEAD_MIN, MemorySize.ZERO);
		config.set(TaskManagerOptions.JVM_OVERHEAD_MAX, MemorySize.ZERO);
		return config;
	}

	private class TestingResourceImpl extends ResourcePBImpl {

		private TestingResourceImpl(int memory, int vcore) {
			super();
			setMemory(memory);
			setVirtualCores(vcore);
		}

		@Override
		public int hashCode() {
			return super.hashCode() + 678;
		}
	}
}
