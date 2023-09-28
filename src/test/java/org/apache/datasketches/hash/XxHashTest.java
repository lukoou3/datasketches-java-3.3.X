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

package org.apache.datasketches.hash;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import org.apache.datasketches.memory.Memory;

/**
 * @author Lee Rhodes
 */
public class XxHashTest {

  @Test
  public void longCheck() {
    long seed = 0;
    long hash1 = XxHash.hash(123L, seed);
    System.out.println(hash1);
    long[] arr = new long[1];
    arr[0] = 123L;
    Memory mem = Memory.wrap(arr);
    long hash2 = XxHash.hash(mem, 0, 8, 0);
    assertEquals(hash2, hash1);
  }

  @Test
  public void testXxHashJdk11() {
    /**
     * jdk11运行直接报错，但是引入编译好的jar没问题，莫名其妙
     *         <dependency>
     *             <groupId>org.apache.datasketches</groupId>
     *             <artifactId>datasketches-memory</artifactId>
     *             <version>2.1.0</version>
     *         </dependency>
     */
    long seed = 0;
    long hash1 = com.zdjz.galaxy.sketch.util.XxHash64.hash(123L, seed);
    System.out.println(hash1);
  }
}
