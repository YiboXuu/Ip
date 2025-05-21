package org.example.cquirrel.processor;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.example.cquirrel.model.DataModels.QueryResult;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.stream.Collectors;

/**
 * 通用 Dynamic Top-N ProcessFunction
 * 输入：KeyedStream<String, QueryResult>  （key 写死 "ALL" 可做全局 Top-N）
 * 输出：List<QueryResult> —— 当前的 Top-N 排好序的一整个列表
 */
public class DynamicTopNProcessFunction
        extends KeyedProcessFunction<String, QueryResult, List<QueryResult>> {

    private final int topSize;
    private transient ListState<QueryResult> heapState;

    public DynamicTopNProcessFunction(int topSize) {
        this.topSize = topSize;
    }

    @Override
    public void open(Configuration parameters) {
        heapState = getRuntimeContext().getListState(
                new ListStateDescriptor<>("heapState", QueryResult.class)
        );
    }

    @Override
    public void processElement(QueryResult value,
                               Context ctx,
                               Collector<List<QueryResult>> out) throws Exception {
        // 1) 恢复小顶堆
        PriorityQueue<QueryResult> heap =
                new PriorityQueue<>(Comparator.comparing(QueryResult::getRevenue));
        for (QueryResult qr : heapState.get()) {
            heap.offer(qr);
        }

        // 2) 插入新元素，修剪到 topSize
        heap.offer(value);
        if (heap.size() > topSize) {
            heap.poll();
        }

        // 3) 更新状态
        heapState.clear();
        for (QueryResult qr : heap) {
            heapState.add(qr);
        }

        // 4) 排序并输出一次完整的 Top-N 列表
        List<QueryResult> topN = heap.stream()
                .sorted(
                        Comparator.comparing(QueryResult::getRevenue).reversed()
                                .thenComparing(QueryResult::getO_orderdate)
                )
                .collect(Collectors.toList());
        out.collect(topN);
    }
}
