/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.datasketches.kll;

import java.util.Arrays;

import org.apache.datasketches.QuantilesHelper;

/**
 * Data structure for answering quantile queries based on the samples from KllSketch
 * @author Kevin Lang
 * @author Alexander Saydakov
 */
final class KllFloatsQuantileCalculator {

  private final long n_;
  private final float[] items_;
  private final long[] weights_; //comes in as weights, converted to cumulative weights
  private final int[] levels_;
  private int numLevels_;

  // assumes that all levels are sorted including level 0
  KllFloatsQuantileCalculator(final float[] items, final int[] levels, final int numLevels,
      final long n) {
    n_ = n;
    final int numItems = levels[numLevels] - levels[0];
    items_ = new float[numItems];
    weights_ = new long[numItems + 1]; // one more is intentional
    levels_ = new int[numLevels + 1];
    populateFromSketch(items, levels, numLevels, numItems);
    blockyTandemMergeSort(items_, weights_, levels_, numLevels_);
    QuantilesHelper.convertToPrecedingCummulative(weights_);
  }

  //For testing only. Allows testing of getQuantile without a sketch.
  KllFloatsQuantileCalculator(final float[] items, final long[] weights, final long n) {
    n_ = n;
    items_ = items;
    weights_ = weights; //must be size of items + 1
    levels_ = null;  //not used by test
    numLevels_ = 0;  //not used by test
  }

  private static void blockyTandemMergeSort(final float[] items, final long[] weights,
      final int[] levels, final int numLevels) {
    if (numLevels == 1) { return; }

    // duplicate the input in preparation for the "ping-pong" copy reduction strategy.
    final float[] itemsTmp = Arrays.copyOf(items, items.length);
    final long[] weightsTmp = Arrays.copyOf(weights, items.length); // don't need the extra one here

    blockyTandemMergeSortRecursion(itemsTmp, weightsTmp, items, weights, levels, 0, numLevels);
  }

  private static void blockyTandemMergeSortRecursion(
      final float[] itemsSrc, final long[] weightsSrc,
      final float[] itemsDst, final long[] weightsDst,
      final int[] levels, final int startingLevel, final int numLevels) {
    if (numLevels == 1) { return; }
    final int numLevels1 = numLevels / 2;
    final int numLevels2 = numLevels - numLevels1;
    assert numLevels1 >= 1;
    assert numLevels2 >= numLevels1;
    final int startingLevel1 = startingLevel;
    final int startingLevel2 = startingLevel + numLevels1;
    // swap roles of src and dst
    blockyTandemMergeSortRecursion(
        itemsDst, weightsDst,
        itemsSrc, weightsSrc,
        levels, startingLevel1, numLevels1);
    blockyTandemMergeSortRecursion(
        itemsDst, weightsDst,
        itemsSrc, weightsSrc,
        levels, startingLevel2, numLevels2);
    tandemMerge(
        itemsSrc, weightsSrc,
        itemsDst, weightsDst,
        levels,
        startingLevel1, numLevels1,
        startingLevel2, numLevels2);
  }

  private static void tandemMerge(
      final float[] itemsSrc, final long[] weightsSrc,
      final float[] itemsDst, final long[] weightsDst,
      final int[] levelStarts,
      final int startingLevel1, final int numLevels1,
      final int startingLevel2, final int numLevels2) {
    final int fromIndex1 = levelStarts[startingLevel1];
    final int toIndex1 = levelStarts[startingLevel1 + numLevels1]; // exclusive
    final int fromIndex2 = levelStarts[startingLevel2];
    final int toIndex2 = levelStarts[startingLevel2 + numLevels2]; // exclusive
    int iSrc1 = fromIndex1;
    int iSrc2 = fromIndex2;
    int iDst = fromIndex1;

    while (iSrc1 < toIndex1 && iSrc2 < toIndex2) {
      if (itemsSrc[iSrc1] < itemsSrc[iSrc2]) {
        itemsDst[iDst] = itemsSrc[iSrc1];
        weightsDst[iDst] = weightsSrc[iSrc1];
        iSrc1++;
      } else {
        itemsDst[iDst] = itemsSrc[iSrc2];
        weightsDst[iDst] = weightsSrc[iSrc2];
        iSrc2++;
      }
      iDst++;
    }
    if (iSrc1 < toIndex1) {
      System.arraycopy(itemsSrc, iSrc1, itemsDst, iDst, toIndex1 - iSrc1);
      System.arraycopy(weightsSrc, iSrc1, weightsDst, iDst, toIndex1 - iSrc1);
    } else if (iSrc2 < toIndex2) {
      System.arraycopy(itemsSrc, iSrc2, itemsDst, iDst, toIndex2 - iSrc2);
      System.arraycopy(weightsSrc, iSrc2, weightsDst, iDst, toIndex2 - iSrc2);
    }
  }

  float getQuantile(final double rank) {
    final long pos = QuantilesHelper.posOfRank(rank, n_);
    return approximatelyAnswerPositonalQuery(pos);
  }

  private float approximatelyAnswerPositonalQuery(final long pos) {
    assert pos >= 0;
    assert pos < n_;
    final int index = QuantilesHelper.chunkContainingPos(weights_, pos);
    return items_[index];
  }

  private void populateFromSketch(final float[] srcItems, final int[] srcLevels,
      final int numLevels, final int numItems) {
    final int offset = srcLevels[0];
    System.arraycopy(srcItems, offset, items_, 0, numItems);
    int srcLevel = 0;
    int dstLevel = 0;
    long weight = 1;
    while (srcLevel < numLevels) {
      final int fromIndex = srcLevels[srcLevel] - offset;
      final int toIndex = srcLevels[srcLevel + 1] - offset; // exclusive
      if (fromIndex < toIndex) { // if equal, skip empty level
        Arrays.fill(weights_, fromIndex, toIndex, weight);
        levels_[dstLevel] = fromIndex;
        levels_[dstLevel + 1] = toIndex;
        dstLevel++;
      }
      srcLevel++;
      weight *= 2;
    }
    weights_[numItems] = 0;
    numLevels_ = dstLevel;
  }

}
