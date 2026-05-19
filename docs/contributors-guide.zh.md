---
title: 贡献者指南
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

# 贡献者指南

本指南将详细介绍作为 SpatialBench 贡献者，应如何搭建开发环境。

## Fork 并克隆仓库

第一步是创建仓库的个人副本，并将其与主项目关联起来。

1. Fork 仓库

   * 访问官方 [SpatialBench GitHub 仓库](https://github.com/apache/sedona-spatialbench)。
   * 点击右上角的 **Fork** 按钮，这会在你的 GitHub 账号下创建一份完整的项目副本。

2. 克隆你的 fork

   * 接下来，将你刚刚 fork 的仓库克隆到本地。该命令会将仓库下载到名为 `sedona-spatialbench` 的新目录中。
   * 请将 `YourUsername` 替换为你实际的 GitHub 用户名。

    ```shell
    git clone https://github.com/YourUsername/sedona-spatialbench.git
    cd sedona-spatialbench
    ```

3. 配置 remote

   * 你的本地仓库需要知道原始项目的位置，以便后续拉取更新。我们通常会添加一个名为 upstream 的远程地址，指向 SpatialBench 主仓库。
   * 你的 fork 会被自动配置为 origin 远程。

    ```shell
    # 将主仓库添加为名为 "upstream" 的远程
    git remote add upstream https://github.com/apache/sedona-spatialbench.git
    ```

4. 验证配置

   * 运行以下命令，确认你正确地配置了两个远程：origin（你的 fork）和 upstream（主仓库）。

    ```shell
    git remote -v
    ```

   * 输出应当类似于：

    ```shell
    origin    https://github.com/YourUsername/sedona-spatialbench.git (fetch)
    origin    https://github.com/YourUsername/sedona-spatialbench.git (push)
    upstream  https://github.com/apache/sedona-spatialbench.git (fetch)
    upstream  https://github.com/apache/sedona-spatialbench.git (push)
    ```

## 开发环境搭建

SpatialBench 使用 Rust 编写，并采用标准的 cargo workspace。你可以从 rustup.rs 安装一个较新的 Rust 编译器和 cargo。

运行测试：

```shell
cargo test
```

可以使用以下命令运行 CLI 的本地开发版本：

```shell
cargo run --bin spatialbench-cli
```

## 调试

### IDE

调试 Rust 代码最方便的方式是编写或定位一个能够触发目标行为的测试，然后在 IDE 中通过 [rust-analyzer](https://www.jetbrains.com/help/fleet/using-rust-analyzer.html) 扩展使用 Debug 模式运行该测试。

### CLI 的详细输出

调试 SpatialBench CLI 时，可以启用详细输出，以查看更详细的日志：

启用详细输出（info 级别日志）：

```shell
cargo run --bin spatialbench-cli -- --scale-factor 1 --verbose
```

或者通过环境变量进行更精细的控制：

```shell
RUST_LOG=debug cargo run --bin spatialbench-cli -- --scale-factor 1
```

`--verbose` 标志会将日志级别设置为 info，并忽略 RUST_LOG 环境变量。如果未指定 `--verbose`，则日志通过 `RUST_LOG` 进行配置。

### 日志级别

你可以使用 `RUST_LOG` 控制日志的粒度：

```shell
# 仅显示错误
RUST_LOG=error cargo run --bin spatialbench-cli -- --scale-factor 1

# 显示警告和错误
RUST_LOG=warn cargo run --bin spatialbench-cli -- --scale-factor 1

# 显示 info、警告和错误
RUST_LOG=info cargo run --bin spatialbench-cli -- --scale-factor 1

# 显示调试输出
RUST_LOG=debug cargo run --bin spatialbench-cli -- --scale-factor 1

# 显示 trace 输出（非常详细）
RUST_LOG=trace cargo run --bin spatialbench-cli -- --scale-factor 1

# 仅显示特定模块的调试输出
RUST_LOG=spatialbench=debug cargo run --bin spatialbench-cli -- --scale-factor 1
```

## 测试

我们使用 cargo 来运行 Rust 测试：

```shell
cargo test
```

也可以仅对某个 crate 运行测试：

```shell
cd spatialbench
cargo test
```

## 代码风格检查

安装 pre-commit。它会自动运行通过 CI 所必需的各项检查（例如代码格式化）：

```shell
pre-commit install
```

此外，在推送新的 Rust 改动前，你还应当运行 clippy 来发现常见的代码问题。该检查不包含在 pre-commit 中，因此需要手动运行。修复它给出的所有建议后，再运行一次以确认没有其他需要修改的地方：

```shell
cargo clippy
```

## 文档

为 SpatialBench 文档做贡献的步骤如下：

1. 克隆仓库并创建一个 fork。
2. 安装文档相关依赖：
    ```shell
    pip install -r docs/requirements.txt
    ```
3. 修改文档文件。
4. 使用以下命令在本地预览改动：
   * `mkdocs serve` —— 启动支持实时刷新的文档服务器。
   * `mkdocs build` —— 构建文档站点。
   * `mkdocs -h` —— 打印帮助信息并退出。
5. 推送改动并提交 Pull Request。
