---
title: SpatialBench Data Distributions
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


SpatialBench offers a set of spatial distributions to generate synthetic datasets with different levels of skew and realism. Each distribution has its own mathematical foundation, parameters, and characteristic spatial patterns. The choice of distribution directly determines whether your data looks like evenly spaced dots on a map, concentrated hotspots, or layered urban clusters.


## Uniform

The simplest case: every point is drawn independently from a uniform distribution in the unit square [0,1]^2.

$$
X \sim U(0,1), \quad Y \sim U(0,1)
$$

There are no parameters to adjust here. The result is an even, flat distribution — useful as a baseline, but one that rarely resembles any real-world spatial dataset. If your goal is to test systems without the confounding factor of skew, this is the place to start.


## Normal

The normal distribution introduces clustering. Both coordinates are drawn from a Gaussian with configurable mean and standard deviation:

$$
X, Y \sim \mathcal{N}(\mu, \sigma^2), \quad \text{clamped to } [0,1]
$$

Here, `mu` determines where the hotspot sits in the square, while `sigma` sets the spread - a small `sigma` produces a sharp, dense cluster, while a larger sigma spreads points more thinly across space. This is appropriate if you want to mimic a single dense center of activity, like one city in an otherwise empty region. The tradeoff is that it’s too simplistic for modeling multiple hotspots or urban complexity.


## Diagonal

The diagonal distribution forces correlation between x and y. With probability percentage, a point is placed exactly on the line y=x. Otherwise, it is perturbed by Gaussian noise with width controlled by buffer. The result is a band of points hugging the diagonal.

This pattern is not realistic geographically, but it is useful for experiments that need a known correlation structure — for example, seeing how indexing or filtering behaves when coordinates are not independent.


## Bit

Bit distributions use recursive binary subdivision of the square. Each bit position is toggled with probability `probability`, and the depth of recursion is determined by `digits`. This produces coordinates that fall into a deterministic grid structure, with cells that may or may not be occupied depending on the randomness of the bits.

The result looks like a lattice of points at varying resolutions. Increasing digits refines the grid; lowering probability sparsifies it. This distribution is intentionally synthetic, good for stress-testing systems against very regular data.


## Sierpinski

Sierpinski patterns come from iterating the “chaos game” toward the vertices of a triangle. After many steps, the points fall into the classic self-similar fractal: a carpet of nested triangular holes. There are no parameters to tune here.

While this is not meant to reflect any natural process, it does generate extreme skew — dense regions interspersed with large gaps — making it a good way to see how systems handle pathological clustering.


## Thomas Process

The Thomas (Gaussian Neyman–Scott) process generates hotspots by layering parent and offspring points. Parent centers are placed deterministically using a Halton sequence. Each parent is assigned a weight drawn from a Pareto distribution, then spawns offspring distributed around it with Gaussian noise of standard deviation sigma.

Key parameters:

- `parents` sets how many hotspots exist overall.
- `mean_offspring` scales the global density.
- `sigma` controls how spread out each cluster is.
- `pareto_alpha` and `pareto_xm` shape the skew in cluster sizes: small alpha values mean a few parents dominate with very large clusters, while most parents remain small.

The result is a landscape of uneven hotspots - some bustling, others barely populated. This makes it much closer to real-world trip or building distributions than uniform or normal alone.


## Hierarchical Thomas

The Hierarchical (or Nested) Thomas process extends the idea by introducing two levels. First, a “city” is selected, with city weights drawn from a Pareto distribution. Within the chosen city, the number of subclusters (neighborhoods) is itself random — normally distributed around a mean with given variance and bounded by min/max limits. Finally, a subcluster is picked (again Pareto-weighted), and the final point is drawn from a Gaussian around that subcluster.

The parameters mirror this structure:

- `cities` controls the number of top-level hubs.
- `sub_mean`, `sub_sd`, `sub_min`, `sub_max` govern how many neighborhoods each city has.
- `sigma_city` spreads neighborhoods around the city center; `sigma_sub` spreads points around a neighborhood.
- The `pareto_alpha`/`pareto_xm` pairs separately skew city sizes and neighborhood sizes.

This distribution produces realistic multi-scale patterns: large cities with many dense neighborhoods, small towns with just a few sparse clusters. It captures the layered heterogeneity of real settlement data in a way no single-level process can.

## References

- **Spider distributions (Uniform, Normal, Bit, Sierpinski, Diagonal):**
     - Puloma Katiyar, Tin Vu, Sara Migliorini, Alberto Belussi, Ahmed Eldawy. *SpiderWeb: A Spatial Data Generator on the Web*. [ACM SIGSPATIAL 2020](https://dl.acm.org/doi/10.1145/3397536.3422351), Seattle, WA.
- **Thomas / Neyman–Scott cluster processes:**
     - Thomas, M. (1949). *A Generalization of Poisson’s Binomial Limit For use in Ecology*. [*Biometrika*, *36*(1/2)](https://doi.org/10.2307/2332526), 18–25.
- Jerzy Neyman, Elizabeth L. Scott, *Statistical Approach to Problems of Cosmology*, [*Journal of the Royal Statistical Society: Series B (Methodological)*, Volume 20, Issue 1, January 1958](https://doi.org/10.1111/j.2517-6161.1958.tb00272.x), Pages 1–29   
- **Point process theory:**
     - Illian, J., Penttinen, A., Stoyan, H., & Stoyan, D. (2008). *Statistical Analysis and Modelling of Spatial Point Patterns*. Wiley.
- **Fractal generation (Sierpinski):**
     - Barnsley, M. F., & Demko, S. (1985). *Iterated function systems and the global construction of fractals*. [Proceedings of the Royal Society of London. Series A, 399(1817)](https://doi.org/10.1098/rspa.1985.0057), 243–275.
