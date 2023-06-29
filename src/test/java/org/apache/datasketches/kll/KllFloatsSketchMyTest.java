package org.apache.datasketches.kll;

import org.testng.annotations.Test;

public class KllFloatsSketchMyTest {

    @Test
    public void test() {
        final KllFloatsSketch sketch = KllFloatsSketch.newHeapInstance();
        for (int i = 0; i < 10000000; i++) {
            sketch.update(i);
        }
        System.out.println(sketch.getQuantile(0.5));
    }


}
