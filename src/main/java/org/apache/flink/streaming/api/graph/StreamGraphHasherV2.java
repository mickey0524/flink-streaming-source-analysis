/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.graph;

import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.transformations.StreamTransformation;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;

import org.apache.flink.shaded.guava18.com.google.common.hash.HashFunction;
import org.apache.flink.shaded.guava18.com.google.common.hash.Hasher;
import org.apache.flink.shaded.guava18.com.google.common.hash.Hashing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import static org.apache.flink.util.StringUtils.byteToHexString;

/**
 * StreamGraphHasher from Flink 1.2. This contains duplicated code to ensure that the algorithm does not change with
 * future Flink versions.
 *
 * <p>DO NOT MODIFY THIS CLASS
 */
/**
 * Flink 1.2 版本的 StreamGraphHasher
 */
public class StreamGraphHasherV2 implements StreamGraphHasher {

	private static final Logger LOG = LoggerFactory.getLogger(StreamGraphHasherV2.class);

	/**
	 * Returns a map with a hash for each {@link StreamNode} of the {@link
	 * StreamGraph}. The hash is used as the {@link JobVertexID} in order to
	 * identify nodes across job submissions if they didn't change.
	 *
	 * <p>The complete {@link StreamGraph} is traversed. The hash is either
	 * computed from the transformation's user-specified id (see
	 * {@link StreamTransformation#getUid()}) or generated in a deterministic way.
	 *
	 * <p>The generated hash is deterministic with respect to:
	 * <ul>
	 *   <li>node-local properties (node ID),
	 *   <li>chained output nodes, and
	 *   <li>input nodes hashes
	 * </ul>
	 * 
	 * hash 要么是从 transformation 的 uid 计算所得，要么是以确定性的方式生成，如下：
	 * 如果定义了 uid，则由 uid 得出，如果没有，由节点本身的属性以及节点的输入，输出得出
	 * @return A map from {@link StreamNode#id} to hash as 16-byte array.
	 */
	@Override
	public Map<Integer, byte[]> traverseStreamGraphAndGenerateHashes(StreamGraph streamGraph) {
		// The hash function used to generate the hash
		// hash 函数用于生成 hash
		final HashFunction hashFunction = Hashing.murmur3_128(0);
		final Map<Integer, byte[]> hashes = new HashMap<>();

		Set<Integer> visited = new HashSet<>();
		Queue<StreamNode> remaining = new ArrayDeque<>();

		// We need to make the source order deterministic. The source IDs are
		// not returned in the same order, which means that submitting the same
		// program twice might result in different traversal, which breaks the
		// deterministic hash assignment.
		// 我们需要让源节点顺序是确定的，如果源节点 id 没有按相同的顺序返回，这意味着提交相同
		// 的程序有可能会得到不同的遍历，会破坏 hash 分配的确定性
		List<Integer> sources = new ArrayList<>();
		for (Integer sourceNodeId : streamGraph.getSourceIDs()) {
			sources.add(sourceNodeId);
		}
		Collections.sort(sources);

		//
		// Traverse the graph in a breadth-first manner. Keep in mind that
		// the graph is not a tree and multiple paths to nodes can exist.
		//
		// 按 BFS 遍历图，需要知道的是图不是一颗树
		// 因此多条路径到一个节点是可能存在的

		// Start with source nodes
		for (Integer sourceNodeId : sources) {
			remaining.add(streamGraph.getStreamNode(sourceNodeId));
			visited.add(sourceNodeId);
		}

		StreamNode currentNode;
		while ((currentNode = remaining.poll()) != null) {
			// Generate the hash code. Because multiple path exist to each
			// node, we might not have all required inputs available to
			// generate the hash code.
			// 生成 hash code。因为对每个节点来说存在多条路径，我们可能没有生成 hash code 所需
			// 的所有 inputs
			if (generateNodeHash(currentNode, hashFunction, hashes, streamGraph.isChainingEnabled(), streamGraph)) {
				// Add the child nodes
				for (StreamEdge outEdge : currentNode.getOutEdges()) {
					StreamNode child = streamGraph.getTargetVertex(outEdge);

					if (!visited.contains(child.getId())) {
						remaining.add(child);
						visited.add(child.getId());
					}
				}
			} else {
				// We will revisit this later.
				// 我们会在随后重新访问该节点
				visited.remove(currentNode.getId());
			}
		}

		return hashes;
	}

	/**
	 * Generates a hash for the node and returns whether the operation was
	 * successful.
	 *
	 * @param node         The node to generate the hash for
	 * @param hashFunction The hash function to use
	 * @param hashes       The current state of generated hashes
	 * @return <code>true</code> if the node hash has been generated.
	 * <code>false</code>, otherwise. If the operation is not successful, the
	 * hash needs be generated at a later point when all input is available.
	 * @throws IllegalStateException If node has user-specified hash and is
	 *                               intermediate node of a chain
	 */
	/**
	 * 生成一个节点的 hash code，返回操作是否成功
	 */
	private boolean generateNodeHash(
			StreamNode node,
			HashFunction hashFunction,
			Map<Integer, byte[]> hashes,
			boolean isChainingEnabled,
			StreamGraph streamGraph) {

		// Check for user-specified ID
		// 检查节点 uid
		String userSpecifiedHash = node.getTransformationUID();

		if (userSpecifiedHash == null) {
			// Check that all input nodes have their hashes computed
			// 检查所有的输入节点是否都计算了 hash code
			for (StreamEdge inEdge : node.getInEdges()) {
				// If the input node has not been visited yet, the current
				// node will be visited again at a later point when all input
				// nodes have been visited and their hashes set.
				// 如果输入节点还没有被访问，当所有的输入节点都被访问过且 hash code 都被设置之后
				// 当前节点会被重新访问
				if (!hashes.containsKey(inEdge.getSourceId())) {
					return false;
				}
			}

			Hasher hasher = hashFunction.newHasher();  // 新生成一个 hash code
			byte[] hash = generateDeterministicHash(node, hasher, hashes, isChainingEnabled, streamGraph);

			if (hashes.put(node.getId(), hash) != null) {
				// Sanity check
				throw new IllegalStateException("Unexpected state. Tried to add node hash " +
						"twice. This is probably a bug in the JobGraph generator.");
			}

			return true;
		} else {
			Hasher hasher = hashFunction.newHasher();  // 新生成一个 hash code
			byte[] hash = generateUserSpecifiedHash(node, hasher);

			// 检查 hash 冲突
			for (byte[] previousHash : hashes.values()) {
				if (Arrays.equals(previousHash, hash)) {
					// 如果冲突的话，很大概率是由于重复的 uid 导致的
					throw new IllegalArgumentException("Hash collision on user-specified ID " +
							"\"" + userSpecifiedHash + "\". " +
							"Most likely cause is a non-unique ID. Please check that all IDs " +
							"specified via `uid(String)` are unique.");
				}
			}

			if (hashes.put(node.getId(), hash) != null) {
				// Sanity check
				throw new IllegalStateException("Unexpected state. Tried to add node hash " +
						"twice. This is probably a bug in the JobGraph generator.");
			}

			return true;
		}
	}

	/**
	 * Generates a hash from a user-specified ID.
	 */
	/**
	 * 从用户定义的 uid 生成 hash code
	 */
	private byte[] generateUserSpecifiedHash(StreamNode node, Hasher hasher) {
		hasher.putString(node.getTransformationUID(), Charset.forName("UTF-8"));

		return hasher.hash().asBytes();
	}

	/**
	 * Generates a deterministic hash from node-local properties and input and
	 * output edges.
	 */
	/**
	 * 由节点本身的性质以及输入输出边生成一个确定性的 hash
	 */
	private byte[] generateDeterministicHash(
			StreamNode node,
			Hasher hasher,
			Map<Integer, byte[]> hashes,
			boolean isChainingEnabled,
			StreamGraph streamGraph) {

		// Include stream node to hash. We use the current size of the computed
		// hashes as the ID. We cannot use the node's ID, because it is
		// assigned from a static counter. This will result in two identical
		// programs having different hashes.
		// 将节点本身加入 hash 的计算。我们使用 hashes 当前的 size 作为 ID
		// 我们不能使用 node 的 ID，因为它是由一个静态 counter 分配 id 的
		// 这会导致相同的程序得到不同的 hashes
		// 如下所示：
		// 范例1：A.id = 1  B.id = 2
		// DataStream<String> A = ...
		// DataStream<String> B = ...
		// A.union(B).print();
		// 范例2：A.id = 2  B.id = 1
		// DataStream<String> B = ...
		// DataStream<String> A = ...
		// A.union(B).print();
		// 上面的两个 job 是完全一样的拓扑，但是 source 的 id 却不一样
		generateNodeLocalHash(hasher, hashes.size());

		// Include chained nodes to hash
		// 将链接的后续节点加入 hash
		for (StreamEdge outEdge : node.getOutEdges()) {
			if (isChainable(outEdge, isChainingEnabled, streamGraph)) {

				// Use the hash size again, because the nodes are chained to
				// this node. This does not add a hash for the chained nodes.
				generateNodeLocalHash(hasher, hashes.size());
			}
		}

		byte[] hash = hasher.hash().asBytes();

		// Make sure that all input nodes have their hash set before entering
		// this loop (calling this method).
		// 确保进入该方法的时候，所有的 input nodes 都有自己的 hash code
		for (StreamEdge inEdge : node.getInEdges()) {
			byte[] otherHash = hashes.get(inEdge.getSourceId());

			// Sanity check
			if (otherHash == null) {
				throw new IllegalStateException("Missing hash for input node "
						+ streamGraph.getSourceVertex(inEdge) + ". Cannot generate hash for "
						+ node + ".");
			}

			// 将输入节点的 hash 加入计算
			for (int j = 0; j < hash.length; j++) {
				hash[j] = (byte) (hash[j] * 37 ^ otherHash[j]);
			}
		}

		if (LOG.isDebugEnabled()) {
			String udfClassName = "";
			if (node.getOperator() instanceof AbstractUdfStreamOperator) {
				udfClassName = ((AbstractUdfStreamOperator<?, ?>) node.getOperator())
						.getUserFunction().getClass().getName();
			}

			LOG.debug("Generated hash '" + byteToHexString(hash) + "' for node " +
					"'" + node.toString() + "' {id: " + node.getId() + ", " +
					"parallelism: " + node.getParallelism() + ", " +
					"user function: " + udfClassName + "}");
		}

		return hash;
	}

	/**
	 * Applies the {@link Hasher} to the {@link StreamNode} . The hasher encapsulates
	 * the current state of the hash.
	 *
	 * <p>The specified ID is local to this node. We cannot use the
	 * {@link StreamNode#id}, because it is incremented in a static counter.
	 * Therefore, the IDs for identical jobs will otherwise be different.
	 */
	private void generateNodeLocalHash(Hasher hasher, int id) {
		// This resolves conflicts for otherwise identical source nodes. BUT
		// the generated hash codes depend on the ordering of the nodes in the
		// stream graph.
		// 这将解决其他相同源节点的冲突
		// 但是这种生成 hash code 的方法依赖 stream graph 节点的顺序
		hasher.putInt(id);
	}

	/**
	 * 判断是否是链式的
	 */
	private boolean isChainable(StreamEdge edge, boolean isChainingEnabled, StreamGraph streamGraph) {
		StreamNode upStreamVertex = streamGraph.getSourceVertex(edge);  // 获取边的源节点
		StreamNode downStreamVertex = streamGraph.getTargetVertex(edge);  // 获取边的目标节点

		StreamOperator<?> headOperator = upStreamVertex.getOperator();  // 获取源头节点的操作符
		StreamOperator<?> outOperator = downStreamVertex.getOperator();  // 获取目标节点的操作符

		return downStreamVertex.getInEdges().size() == 1  // 目标节点的入度为 1
				&& outOperator != null  // 目标节点操作符不能为空
				&& headOperator != null  // 源节点操作符不能为空
				&& upStreamVertex.isSameSlotSharingGroup(downStreamVertex)  // 源头节点和目标节点的 slot sharing group 相同
				&& outOperator.getChainingStrategy() == ChainingStrategy.ALWAYS
				&& (headOperator.getChainingStrategy() == ChainingStrategy.HEAD || // HEAD 模式允许后续节点链式连接
				headOperator.getChainingStrategy() == ChainingStrategy.ALWAYS)
				&& (edge.getPartitioner() instanceof ForwardPartitioner)
				&& upStreamVertex.getParallelism() == downStreamVertex.getParallelism()
				&& isChainingEnabled;
	}
}
