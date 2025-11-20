package bean;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;

public class ReservoirSampler<T extends Comparable<T>> {
    private final int k; // 蓄水池大小
    private final ArrayList<T> reservoir; // 使用 ArrayList 存储采样数据
    private final Random random;
    private int count = 0; // 到目前为止处理的元素数量

    public ReservoirSampler(int k) {
        this.k = k;
        this.reservoir = new ArrayList<>(k);
        this.random = new Random(2024);
    }

    public synchronized void add(T newItem) {
        count++;
        if (count <= k) {
            reservoir.add(newItem);
        } else {
            int replaceIndex = random.nextInt(count);
            if (replaceIndex < k) {
                reservoir.set(replaceIndex, newItem);
            }
        }
    }

    public void sortForInitialize() {
        Collections.sort(reservoir);
    }

    public synchronized ArrayList<T> getSortedSample() {
        Collections.sort(reservoir);
        return new ArrayList<>(reservoir);
    }

    public T getRandomItem() {
        if (reservoir.isEmpty()) {
            return null;  // 如果蓄水池为空，返回 null
        }
        int randomIndex = random.nextInt(reservoir.size()); // 在蓄水池的大小范围内生成一个随机索引
        return reservoir.get(randomIndex); // 返回随机索引对应的元素
    }

    public ArrayList<T> getPercentileList(double percentile) {
        if (percentile < 0 || percentile > 1) {
            throw new IllegalArgumentException("Percentile must be between 0 and 1.");
        }
        ArrayList<T> sortedSample = getSortedSample();
        if (sortedSample.isEmpty()) {
            return null;
        }
        int index = (int) Math.ceil(percentile * sortedSample.size()) - 1;
        index = Math.max(0, index);
        index = Math.min(index, sortedSample.size() - 1);
        ArrayList<T> result = new ArrayList<>();
        result.add(sortedSample.get(index));
        result.add(sortedSample.get(sortedSample.size() - 1));
        return result;
    }

    public T getPercentile(double percentile) {
        if (percentile < 0 || percentile > 1) {
            throw new IllegalArgumentException("Percentile must be between 0 and 1.");
        }
        ArrayList<T> sortedSample = getSortedSample();
        if (sortedSample.isEmpty()) {
            return null;
        }
        // System.out.println(percentile);
        int index = (int) Math.ceil(percentile * sortedSample.size());
        index = Math.max(0, index);
        index = Math.min(index, sortedSample.size() - 1);
        // System.out.println("current index is: "+ index);
        return sortedSample.get(index);
    }
}