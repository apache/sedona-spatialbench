---
title: SpatialBench 数据分布
---

<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->


SpatialBench 提供了一组空间分布，用于生成具有不同偏斜程度和真实度的合成数据集。每种分布都有其独特的数学基础、参数以及典型的空间模式。所选分布直接决定了你的数据看起来是地图上均匀散布的点、集中的热点，还是分层的城市聚类。


## Uniform（均匀分布）

最简单的情形：每个点独立地从单位正方形 $[0,1]^2$ 上的均匀分布中抽取。

$$
X \sim U(0,1), \quad Y \sim U(0,1)
$$

该分布没有可调参数。结果是均匀、平坦的分布——可以作为基线使用，但很少能反映任何真实的空间数据集。如果你的目标是在不引入偏斜干扰的情况下测试系统，那么这是一个不错的起点。


## Normal（正态分布）

正态分布会引入聚集特征。两个坐标都从可配置均值和标准差的高斯分布中抽取：

$$
X, Y \sim \mathcal{N}(\mu, \sigma^2), \quad \text{截断至 } [0,1]
$$

其中 `mu` 决定热点在正方形中的位置，`sigma` 决定扩散程度——较小的 `sigma` 会产生一个尖锐密集的聚类，较大的 sigma 则会使点更稀疏地分布在空间中。如果你希望模拟单个高密度活动中心（例如，在大片空白区域中只有一座城市的情况），这是合适的选择。其代价是它过于简单，无法建模多个热点或复杂的城市结构。


## Diagonal（对角线分布）

对角线分布在 x 与 y 之间强制引入相关性。以一定概率（probability）将点直接放置在 $y=x$ 的直线上，否则就用宽度由 buffer 控制的高斯噪声对点进行扰动。结果是一条紧贴对角线的点带。

这种模式在地理上并不真实，但对于需要已知相关结构的实验非常有用——例如，观察当坐标不独立时，索引或过滤的行为表现。


## Bit（比特分布）

比特分布通过对正方形进行递归二进制细分。每个比特位以概率 `probability` 切换，递归深度由 `digits` 决定。这会生成一组落入确定性网格结构的坐标，单元格是否被占据则取决于比特随机性。

结果看起来像一个不同分辨率的点阵。增大 digits 可以细化网格；降低 probability 则会让网格更稀疏。这种分布本质上是合成的，适用于对系统进行高度规整数据的压力测试。


## Sierpinski（谢尔宾斯基分布）

谢尔宾斯基模式来自“混沌游戏”，朝着三角形顶点反复迭代。经过多次迭代后，点会落入经典的自相似分形：一张布满嵌套三角形空洞的“地毯”。该分布没有可调参数。

虽然这种分布并不模拟任何自然过程，但它能生成极度偏斜的数据——密集区与大块空白交替出现，因此非常适合用来观察系统在面对病态聚集时的表现。


## Thomas 过程

Thomas（高斯型 Neyman–Scott）过程通过分层生成父点和子点来产生热点。父点中心使用 Halton 序列进行确定性放置。每个父点会被分配一个从帕累托分布中抽取的权重，然后围绕该父点按标准差为 sigma 的高斯噪声生成子点。

关键参数：

- `parents` 决定整体有多少个热点。
- `mean_offspring` 用于缩放全局密度。
- `sigma` 控制每个聚类的扩散程度。
- `pareto_alpha` 与 `pareto_xm` 共同塑造聚类大小的偏斜度：较小的 alpha 值意味着少数父点拥有极大的聚类，而大多数父点的聚类规模较小。

结果是一幅热点不均匀的图景——有些极为繁忙，有些几乎无人。这使得该分布比单纯的均匀分布或正态分布更接近真实的行程或建筑物分布。


## Hierarchical Thomas（层次化 Thomas）

层次化（或嵌套）Thomas 过程在前者的基础上引入了两个层级。首先，按照从帕累托分布中抽取的城市权重选择一个“城市”。在被选中的城市内，子聚类（街区）的数量本身是随机的——服从均值给定、方差给定、并被 min/max 约束的正态分布。最后，再（同样按帕累托权重）选择一个子聚类，最终点从围绕该子聚类的高斯分布中抽取。

参数也呼应了这一结构：

- `cities` 控制顶层枢纽（城市）的数量。
- `sub_mean`、`sub_sd`、`sub_min`、`sub_max` 决定每个城市拥有多少个街区。
- `sigma_city` 控制街区围绕城市中心的分散程度；`sigma_sub` 控制点围绕街区的分散程度。
- 两组 `pareto_alpha`/`pareto_xm` 分别对城市规模和街区规模的偏斜度进行建模。

这种分布可以产生真实的多尺度模式：大城市拥有大量密集的街区，而小城镇只有少数稀疏的聚类。它能以单层过程无法实现的方式，捕捉真实聚居数据中分层的异质性。

## 参考文献

- **Spider 分布（Uniform、Normal、Bit、Sierpinski、Diagonal）：**
     - Puloma Katiyar, Tin Vu, Sara Migliorini, Alberto Belussi, Ahmed Eldawy. *SpiderWeb: A Spatial Data Generator on the Web*. [ACM SIGSPATIAL 2020](https://dl.acm.org/doi/10.1145/3397536.3422351), Seattle, WA.
- **Thomas / Neyman–Scott 聚类过程：**
     - Thomas, M. (1949). *A Generalization of Poisson’s Binomial Limit For use in Ecology*. [*Biometrika*, *36*(1/2)](https://doi.org/10.2307/2332526), 18–25.
- Jerzy Neyman, Elizabeth L. Scott, *Statistical Approach to Problems of Cosmology*, [*Journal of the Royal Statistical Society: Series B (Methodological)*, Volume 20, Issue 1, January 1958](https://doi.org/10.1111/j.2517-6161.1958.tb00272.x), Pages 1–29
- **点过程理论：**
     - Illian, J., Penttinen, A., Stoyan, H., & Stoyan, D. (2008). *Statistical Analysis and Modelling of Spatial Point Patterns*. Wiley.
- **分形生成（Sierpinski）：**
     - Barnsley, M. F., & Demko, S. (1985). *Iterated function systems and the global construction of fractals*. [Proceedings of the Royal Society of London. Series A, 399(1817)](https://doi.org/10.1098/rspa.1985.0057), 243–275.
