package baidu.scloud.data.aggregate.heap;

/**
 * create min heap and heapify heap 
 * @param T: heap basic data 
 */
public class MinHeap<T extends HeapData> {
	private T[] data;

	public MinHeap(T[] data) {
		this.data = data;
		buildHeap();
	}

	private void buildHeap() {
		for (int i = (data.length) / 2 - 1; i >= 0; i--) {
			heapify(i);
		}
	}

	private void heapify(int i) {
		int l = left(i);
		int r = right(i);

		int smallest = i;

		if (l < data.length && data[l].less_override(data[i]))
			smallest = l;

		if (r < data.length && data[r].less_override(data[smallest]))
			smallest = r;

		if (i == smallest)
			return;

		swap(i, smallest);

		heapify(smallest);
	}

	private int right(int i) {
		return (i + 1) << 1;
	}

	private int left(int i) {
		return ((i + 1) << 1) - 1;
	}

	private void swap(int i, int j) {
		T tmp = data[i];
		data[i] = data[j];
		data[j] = tmp;
	}

	public T getRoot() {
		return data[0];
	}

	public void setRoot(T root) {
		data[0] = root;
		heapify(0);
	}
}