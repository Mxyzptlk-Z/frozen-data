<!-- <p align="center">
  <img src="docs/source/_static/img/logo/logo_h_trans.png" width="400" height="103"/>
</p> -->

<p align="center">
  <img src="docs/source/_static/img/logo/logo_v_trans.png" width="220" height="200"/>
</p>

---

<div align="center">
  <a href="https://pypi.org/project/frozen-quant/#files">
    <img src="https://img.shields.io/pypi/pyversions/frozen-quant?logo=python&logoColor=white&color=%23C0CDDE" alt="Python Versions">
  </a>
  <a href="https://pypi.org/project/frozen-quant/#files">
    <img src="https://img.shields.io/badge/platform-linux%20%7C%20windows%20%7C%20macos-%23E5ECE3" alt="Platform">
  </a>
  <a href="LICENSE">
    <img src="https://img.shields.io/pypi/l/frozen-quant?color=%23DEEDFF" alt="License">
  </a>
</div>

Frozen (**F**ast **R**esearch **O**ptimi**z**atin **En**gine) is an advanced factor-driven quantitative research platform, powered by a sophisticated event-driven backtesting engine and cutting-edge portfolio optimization engine. The framework aims to seamlessly integrate data processing, research analytics, back-testing, and live-trading into a unified pipeline, while standardizing factor research procedures through a systematic and rigorous approach.

The framework covers the entire chain of quantitative investment: alpha seeking, risk modeling, portfolio optimization, and order execution, enabling researchers and practitioners to efficiently transform their investment hypotheses into implementable strategies. With Frozen's robust architecture and intuitive interface, users can rapidly prototype, validate, and deploy sophisticated quantitative investment strategies with institutional-grade reliability.

## 📰 **What's NEW!** &nbsp;   ✨

Recent released features

| Feature                                   | Status                                                                                                                                                                                                                       |
| ----------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Data ETL pipeline                         | 🔨[Released](https://github.com/Mxyzptlk-Z/frozen/releases/tag/v3.2.0) on Nov 3, 2024                                                                                                                                           |
| Parameter tuning by optuna                | 🔨[Released](https://github.com/Mxyzptlk-Z/frozen/releases/tag/v3.2.0) on Oct 23, 2024                                                                                                                                         |
| Release Frozen v3.0.0                     | 📈[Released](https://github.com/Mxyzptlk-Z/frozen/releases/tag/v3.0.0) on Sep 15, 2024                                                                                                                                          |
| Integrate Qlib machine learning extension | 📈[Released](https://github.com/Mxyzptlk-Z/frozen/releases/tag/v2.9.0) on Aug 20, 2024                                                                                                                                          |
| ETF backtest engine                      | 📈[Released](https://github.com/Mxyzptlk-Z/frozen/releases/tag/v2.8.0) on Jul 21, 2024                                                                                                                                         |
| Factor database                           | 🔨[Released](https://github.com/Mxyzptlk-Z/frozen/releases/tag/v2.7.0) on Jun 29, 2024                                                                                                                                         |
| Release Frozen v2.6.0                     | 📈[Released](https://github.com/Mxyzptlk-Z/frozen/releases/tag/v2.6.0) on Jun 13, 2024                                                                                                                                          |
| DuckDB support for data module            | 📈[Released](https://github.com/Mxyzptlk-Z/frozen/releases/tag/v2.6.0) on May 14, 2024                                                                                                                                         |
| 3D-GPlearn factor mining extension        | :octocat: [Released](https://github.com/Mxyzptlk-Z/frozen/releases/tag/v2.5.0) on Apr 7, 2024 |
| Integrate Qlib factor expression engine   | 📈[Released](https://github.com/Mxyzptlk-Z/frozen/releases/tag/v2.4.0) on Feb 25, 2024                                                                                                                                         |
| Factor expression engine                  | 🔨[Released](https://github.com/Mxyzptlk-Z/frozen/releases/tag/v2.3.0) on Feb 17, 2024                                                                                                                                         |
| Parallel computation example              | 🔨[Released](https://github.com/Mxyzptlk-Z/frozen/releases/tag/v2.1.0) on Jan 14, 2024                                                                                                                                         |
| Release Frozen v2.0.0                     | 📈[Released](https://github.com/Mxyzptlk-Z/frozen/releases/tag/v2.0.0) on Dec 18, 2023                                                                                                                                          |
| Release Frozen v1.0.0                    | 📈[Released](https://github.com/Mxyzptlk-Z/frozen/releases/tag/v1.0.0-alpha) on Apr 24 2023                                                                                                                                   |

## 📑 **Contents** &nbsp;   ❄️

Document bookmarks

<table>
  <tbody>
    <tr>
      <th>Frameworks, Tutorial, Data & DevOps</th>
      <th>Solutions in Auto Quant Research</th>
    </tr>
      <td>
        <li><a href="#plans"><strong>Plans</strong></a></li>
        <li><a href="#framework-of-frozen">Framework of Frozen</a></li>
        <li><a href="#quick-start">Quick Start</a></li>
          <ul dir="auto">
            <li type="circle"><a href="#installation">Installation</a></li>
            <li type="circle"><a href="#data-preparation"><strong>Data Preparation</strong></a></li>
            <li type="circle"><a href="#factor-research"><strong>Factor Research</strong></a></li>
              <ul dir="auto">
                <li type="circle"><a href="#formulaic-alpha">Formulaic Alpha</a></li>
                <li type="circle"><a href="#factor-mining">Factor Mining</a></li>
                <li type="circle"><a href="#factor-database">Factor Database</a></li></ul>
            <li type="circle"><a href="#investment-strategy"><strong>Investment Strategy</strong></a></li>
              <ul dir="auto">
                <li type="circle"><a href="#backtest-assumption">Backtest Assumption</a></li>
                <li type="circle"><a href="#portfolio-optimization">Portfolio Optimization</a></li>
                <li type="circle"><a href="#parameter-tuning">Paramemter Tuning</a></li></ul>
            <li type="circle"><a href="#live-trading"><strong>Live Trading</strong></a></li>
              <ul dir="auto">
                <li type="circle"><a href="#trading-platform">Trading Platform</a></li>
                <li type="circle"><a href="#order-execution-algorithm">Order Execution Algorithm</a></li>
                <li type="circle"><a href="#risk-control">Risk Control</a></li></ul>
          </ul>
        <li><a href="#more-about-frozen">More About Frozen</a></li>
          <ul dir="auto">
            <li type="circle"><a href="#pros-and-cons">Pros and Cons</a></li>
              <!-- <ul dir="auto">
                <li type="circle"><a href="#our-strengths">Our Strengths</a></li>
                <li type="circle"><a href="#our-weaknesses">Our Weaknesses</a></li></ul> -->
            <li type="circle"><a href="#documentation">Documentation</a></li>
            <li type="circle"><a href="#related-reports">Related Reports</a></li></ul>
          <li><a href="#legal">Legal</a>
            <ul dir="auto">
              <li type="circle"><a href="#license">License</a></li>
              <li type="circle"><a href="#disclaimer">Disclaimer</a></li>
            </ul>
          <!-- <li><a href="#contact-us">Contact Us</a></li> -->
      </td>
      <td valign="baseline">
        <li><a href="#general-research-workflow">General Research Workflow</a>
          <ul dir="auto">
            <li type="circle"><a href="#data-etl-pipeline">Data ETL Pipeline</a></li>
              <ul dir="auto">
                <li type="circle"><a href="#workflow-orchestration">Workflow Orchestration</a></li>
                <li type="circle"><a href="#scheduled-update">Scheduled Update</a></li></ul>
                <li type="circle"><a href="#factor-research-workflow">Factor Research Workflow</a></li>
              <ul dir="auto">
                <li type="circle"><a href="#factor-calculation">Factor Calculation</a></li>
                <li type="circle"><a href="#factor-evaluation">Factor Evaluation</a></li>
                <li type="circle"><a href="#factor-registration">Factor Registration</a></li></ul>
            <li type="circle"><a href="#strategy-implementation">Strategy Implementation</a></li>
              <ul dir="auto">
                <li type="circle"><a href="#portfolio-construction">Portfolio Construction</a></li>
                <li type="circle"><a href="#trading-signal-generation">Trading Signal Generation</a></li>
                <li type="circle"><a href="#performance-assessment">Performance Assessment</a></li></ul>
            <li type="circle"><a href="#trade-live">Trade Live</a></li>
            <li type="circle"><a href="#customized-research-workflow">Customized Research Workflow</a></li>
              <ul dir="auto">
                <li type="circle"><a href="#minimal-strategy">Minimal Strategy</a></li>
                <li type="circle"><a href="#backtest-demo">Backtest demo</a></li>
                <li type="circle"><a href="#run-a-single-factor">Run a single factor</a></li>
                <li type="circle"><a href="#run-multiple-factors">Run multiple factors</a></li></ul>
          </ul>
        </li>
  <li><a href="#auto-quant-research-workflow">Auto Quant Research Workflow</a>
        <li><a href="#interactive-visualization-tools">Interactive Visualization Tools</a>
          <ul dir="auto">
            <li type="circle"><a href="#real-time-monitor-panel">Real-Time Monitor Panel</a></li>
            <li type="circle"><a href="#web-ui-design">Web UI Design</a></li>
            <li type="circle"><a href="#mobile-app">Mobile App</a></li>
          </ul>
        </li>
        <!-- <li><a href="#legal">Legal</a>
          <ul dir="auto">
            <li type="circle"><a href="#license">License</a></li>
            <li type="circle"><a href="#disclaimer">Disclaimer</a></li>
          </ul> -->
        <li><a href="#contact-us">Contact Us</a></li>
      </td>
    </tr>
  </tbody>
</table>

# Plans

New features under development (order by estimated release time).

<!-- | Feature | Status |
| ------- | ------ | -->

# Framework of Frozen

<div style="align: center">
<img src="docs/source/_static/img/framework.png" />
</div>

The high-level framework of Frozen can be found above (users can find the [detailed framework](https://frozen.readthedocs.io/en/latest/introduction/introduction.html#introduction) of Frozen's design when getting into nitty gritty).
The components are designed as loose-coupled modules, and each component could be used stand-alone.

Frozen provides a strong infrastructure to support Quant research. [Data]() is always an important part.
A strong [factor research]() framework is designed to support diverse research paradigms and market patterns at different levels. The machine learning extension succeeds [Qlib](https://github.com/microsoft/qlib) for AI-based factor research. 
By modeling the market, [trading strategies]() will generate trade decisions that will be executed. [Risk model]() prevents the strategy from unforeseen events with portfolio optimization.
At last, a comprehensive [analysis]() will be provided and the model can be [served online]() at a low cost.

# Quick Start

This quick start guide tries to demonstrate

1. It's very easy to build a complete Quant research workflow and try your ideas with _Frozen_.
2. Though with *public data* and *simple models*, traditional factors **work very well** in practical Quant investment.

Here is a quick **[demo]()** shows how to install ``Frozen``, and run Alpha101 strategy with ``frun``. **But**, please make sure you have already prepared the data following the [instruction](#data-preparation).

## Installation

This table demonstrates the supported Python version of `Frozen`:

|             | install with pip | install from source | Data ETL |
| ----------- | :--------------: | :-----------------: | :------: |
| Python 3.9  |       ✔️       |        ✔️        |    ❌    |
| Python 3.10 |       ✔️       |        ✔️        |   ✔️   |
| Python 3.11 |       ✔️       |        ✔️        |   ✔️   |

**Note**:

1. **Conda** is suggested for managing your Python environment. In some cases, using Python outside of a `conda` environment may result in missing header files, causing the installation failure of certain packages.
2. Python introduced match-case expression in version 3.10, which is incorporated in data ETL pipeline. Thus, for python 3.9, users will have to roll back manually to if-else expression.

### Install with pip

Users can easily install ``Frozen`` by pip according to the following command.

```bash
  pip install frozen-quant
```

**Note**: pip will install the latest stable frozen. However, the main branch of frozen is in active development. If you want to test the latest scripts or functions in the main branch. Please install frozen with the methods below.

### Install from source

Also, users can install the latest dev version ``Frozen`` by the source code according to the following steps:

* It is recommended that users create a seperate environment for better management

  ```shell
  # if you prefer to use virtual environment, uncomment the following
  # python -m venv .venv
  # source .venv/bin/activate

  # if you prefer to use conda environment, run below
  conda create -n frozen python=3.11
  ```
* Before installing ``Frozen`` from source, users need to install some dependencies:

  ```bash
  pip install numpy
  pip install --upgrade  cython
  ```
* Download the repository and install ``Frozen`` as follows.

  ```bash
  pip install -e .  # `pip install -e .[dev]` is recommended for development.
  ```

  **Note**:  You can install Frozen with `python setup.py install` as well. But it is not the recommended approach. It will skip `pip` and cause obscure problems. For example, **only** the command ``pip install .`` **can** overwrite the stable version installed by ``pip install frozen-quant``, while the command ``python setup.py install`` **can't**.

## Data Preparation

### Data Source

Frozen integrates Tushare as its primary data provider. The source offers comprehensive access to financial market data and analytics. For detailed API documentation and available endpoints, please consult the official [documentation](https://www.tushare.pro/document/2).

_Additional data providers will be integrated in upcoming releases._

### Data Storage Infrastructure

The system implements a flexible and robust data storage architecture, designed to handle various data types and usage scenarios efficiently.

#### File-based Storage

The relevant data files are carefully managed and stored [here](./frozen/database/file) by default.

* Raw data files

  - Efficient local file system for data organization
  - Support for multiple file formats (CSV, Pickle, Parquet, HDF5)

* Local database files

  - File-based database implementation for structured data
  - Automatic data versioning and backup

*We recommend users to prepare their own data if they have a high-quality dataset. For more information, users can refer to the [related document]()*.

#### Database System

Frozen implements a comprehensive database ecosystem, supporting various database management systems optimized for different use cases.

The framework currently support three types of DBMS: MongoDB, DuckDB and ClickHouse, each tailored for specific purposes:

* Automated data ETL(Extract, Transform, Load) pipeline
* High-speed data access and processing
* Built-in data validation and integrity(consistency) checks
* Support for incremental updates
* Efficient for concurrent operations
* Degree of integration for unstructured data

We recommend users to install `DBeaver` for better database management experience. For detailed information regarding the property of different database management systems, please refer to the [related document]().

---

Here is an example to download the historical data from 20050101 to 20231231, with Tushare as datasource and MongoDB as database.

* Get with module

  ```bash
  # get daily data
  python -m scripts.get_data --source tushare --database mongodb frozen_data --end_date 20231231

  # update daily data
  python -m scripts.get_data --source tushare --database mongodb --update True frozen_data --parallel True
  ```
* Get from source

  ```bash
  # get daily data
  PYTHONPATH=$(pwd) python scripts/get_data.py --source tushare --database mongodb frozen_data --end_date 20231231

  # update daily data
  PYTHONPATH=$(pwd) python scripts/get_data.py --source tushare --database mongodb --update True frozen_data --parallel True
  ```

Load and prepare data by running the following code:

```python
from frozen.database.dataload import dataloader
kbar = dataloader.load_volumn_price("stock_daily_real", ("open", "high", "low", "close"), universe)
```

## Factor Research

### Formulaic Alpha

Formulaic alphas represent a systematic approach to quantitative trading, where mathematical expressions are used to define factors and generate trading signals. the paper [&#34;101 Formulaic Alphas&#34;](https://arxiv.org/vc/arxiv/papers/1601/1601.00991v1.pdf) provided one of the first public collections of real-world trading factors, which is considered a foundational work in factor investment,

Inspired by this idea, Frozen implements a powerful factor expression engine that allows you to write concise factor formulas instead of complex python codes. The specific steps are revealed in [factor calculation](#factor-calculation).

### Factor Mining

Factor mining is also known as formulaic alpha discovery, which is a systematic process of discovering, testing, and validating mathematical expressions that can predict future asset returns.

Frozen incorporates sophisticated factor mining capabilities using Genetic Programming (GP), leveraging and extending the `gplearn` package. While traditional GP implementations like gplearn are limited to 2D inputs, we've enhanced the framework to handle 3D financial data structures:

* Time dimension (datetime)
* Cross-sectional dimension (instruments)
* Feature dimension (price, volume, fundamentals, etc.)

A demo of hands-on application is included in [notebook](./demo/workflow/factor_mining.ipynb). For more detailed information, please refer to [GPlearn3D document](./frozen/factor/extensions/_gplearn3d/README.md).

_Note: Other extensible GP frameworks like `deap` will be incorporated in the future. Besides, Reinforcement Learning techniques will also be included in [ML](./frozen/ml) module._

### Factor Database

Frozen provides a robust infrastructure for managing, storing, and efficiently retrieving factors, significantly optimizes the utilization of valuable computation resources.

The factor library integrates seamlessly with all database backends supported in the [data preparation](#data-preparation) part, offering flexibility in storage solutions. Part of the key features are listed below:

* Systematic factor registration
* Multi-threaded batch insertion capabilities
* Incremental updates of factor value
* Caching mechanism for frequent access

A typical usage example is shown in [factor registration](#factor-registration). For more information on the structure and features of factor library, please refer to [factor document](./frozen/factor/README.md).

## Investment Strategy

Frozen adopts [Top-K drop](./docs/source/_static/img/topk_drop.png) factor strategy for both backtesting and live-trading, where factor values are calculated by the given frequency and the top K instruments are held on each portfolio rebalancing day.

Default strategy parameters and account settings are defined in [config file](./frozen/basis/config/default_config.yaml). Users can customize these settings by configuring their own parameters to override the defaults.

### Backtest Assumption

Frozen backtest engine adopts the following default settings:

- **Signal Generation**: Factor calculations are performed after market close on the day before portfolio rebalancing
- **Order Simulation**: During each holding period, investors buy at the opening price on the start date and sell at the closing price on the end date
- **Transaction Costs**: Commission fees are charged on both buy and sell sides, with fixed stamp duty and slippage
- **Account Capital**: The account supports unlimited capital injection to meet minimal capital (margin) requirements when funds are insufficient.
- **Trading Restrictions**: No limitation on transaction volume, but with minimum trading unit of one lot (e.g., 100 shares)
- **Rebalancing Frequency**: Supports medium to low-frequency strategies (daily or lower frequency)
- **Risk Control Thresholds**: When stock price hits profit-taking/stop-loss thresholds, investors immediately sell at current price
- **Upper Limit Price Handling**: For instruments reaching their upper price limit on the intended purchase day, trades will be executed at the opening price of the subsequent trading day
- **Lower Limit Price Handling**: For positions reaching their lower price limit on the intended sale day, trades will be executed at the opening price of the subsequent trading day

_NOTE: Keep in mind that the real-time market condition is ever-changing, and backtest results are only the best estimate of strategy performance based on these assumptions._

### Portfolio Optimization

Frozen adopts modern portfolio theory (MPT) principles to obtain portfolio rebalancing positions. Specifically, the system implements a [risk model](./frozen/engine/riskmodel/README.md), where Mean-Variance Optimization (MVO) is used to achieve optimal asset allocation weights.

Risk model aims at reaching an optimization target while considering practical constraints such as position limits, asset weight bounds and trade directions to ensure real-world applicability. The model features robust covariance matrix estimation techniques and employs the statistical estimators through a Walk-Forward Optimization (WFO) procedure.

Here's a typical config for portfolio optimization:

```yaml
portfolio_config:
  optimizer: mean-variance
  cov_est:
    cov_method: shrink
    cov_window: 60
  opt_func: sharpe
  long_short: False
```

_Risk model will incorporate `skfolio` for better visualization in the future._

### Parameter Tuning

The parameter space for backtesting is virtually infinite. To address this complexity, Frozen leverages `optuna` framework for parameter optimization tasks at the system level.

The system implements a dual-tiered optimization framework encompassing two distinct tuning methodologies:

* Factor Parameter Tuning
  `FactorTuning` primarily focuses on factor signal quality and stability optimization.
  Example:
  * Objective: Maximize Information Coefficient (IC)
  * Constraint: Turnover threshold restriction

* Strategy Parameter Tuning
  `StrategyTuning` primarily focuses on overall strategy performance optimization.
  Example:
  * Objective: Maximize Sharpe Ratio
  * Constraint: Volatility boundary and transaction cost threshold

It is important to emphasize that parameter tuning is highly susceptible to overfitting when conducted without proper constraints. To avoid overfitting, reasonable restrictions based on prior knowledge or empirical evidence should be imposed. Detailed information is included in the [document](./frozen/utils/README.md).

---

Here's an example to call strategy parameter tuning task by wrapping `FactoryFactory` in `StrategyTuning`:

```python
# initialize strategy
factory = FactorFactory()
factory.calc()
# define parameter space
PARAM_SPACE = {
    'max_instruments': {
        'type': 'int',
        'min': 3,
        'max': 20
    },
    'date_rule': {
        'type': 'categorical',
        'choice': ["W-WED", "2W", "M"]
    },
    "optimizer": {
        "type": "categorical",
        "choice": ["equal-weight", "mean-variance"]
    }
}
# call parameter tuning
opt = StrategyTuning(factory)
opt.autotune(PARAM_SPACE, n_trials=5, n_jobs=-1)
```

Complete demonstration can be found in test scripts: [factor tuning](./demo/workflow/factor_tuning.py), [strategy tuning](./demo/workflow/strategy_tuning.py) and [parameter tuning](./demo/workflow/parameter_tuning.ipynb).

## Live Trading

While backtesting provides historical performance validation, live trading represents the ultimate test of a strategy's effectiveness by executing trades in real-world market conditions. The transition from backtesting to live trading introduces additional complexities including market impact, liquidity constraint, execution costs, and real-time data processing.

### Trading Platform

with Qmt, released in the future

### Order Execution Algorithm

will be released in the future

### Risk Control

will be released in the future

# General Research Workflow

Frozen allows you to setup a complete quantitative research workflow by the following steps:

<div style="align: center">
<img src="docs/source/_static/img/workflow.png" />
</div>

## Data ETL Pipeline

**The first step**: Manage the data.

High-quality data serves as the foundation for all subsequent analyses and development. Furthermore, comprehensive data validation and preprocessing are essential to maintain consistency and reliability throughout the entire pipeline.

The following command demonstrates how to download and update historical data from 20050101 to 20231231, with Tushare as datasource and MongoDB as database.

```bash
drun --source tushare --database duckdb --start_date 20220101 --end_date 20231231 --update False
```

### Workflow Orchestration

Frozen attempts to arrange datafeed workflow with `Prefect`, a workflow orchestration framework.

To start Prefect service, run the following bash command in the terminal:

```bash
# if you want to conduct datafeed task flow orchestration
prefect config set PREFECT_API_URL="http://127.0.0.1:4200/api"
prefect server start
drun --serve True
```

### Scheduled Update

> This step is *Optional* if users only want to try their models and strategies on history data.
>
> It is recommended that users update the data manually once and then set it to update automatically.
>
> **NOTE**: Users should use `tushare` to download api data from scratch and then incrementally update it.

* Automatic update of data to the "file" directory each trading day (Linux)

  * use *crontab*: `crontab -e`
  * set up timed tasks:

    ```
    * * * * 1-5 python <script path> --source <user datasource> --database <user database> --update True frozen_data --start_date <start date> --end_date <end date>
    ```

    * **script path**: *scripts/get_data.py*
* Manual update of data

  ```
  python scripts/get_data.py --source <user datasource> --database <user database> --update True frozen_data --start_date <start date> --end_date <end date>
  ```

  * *start_date*: start of trading day
  * *end_date*: end of trading day (included)

## Factor Research Workflow

**The second step**: Define factor and compute its values.

### Factor Calculation

Write factors (with expression engine) and calculate factor values for pre-defined instrument universe.

Here's an example to reproduce the `alpha001` factor from the paper:

```python
# define instrument universe
universe = ...
# prepare related data
data_definitions = [
    ("stock_daily_hfq", ("close", "pct_chg"), ("close", "returns")),
    ]
data = batchLoader(data_definitions, universe)
# calculate factor
string = "rank(Ts_ArgMax(SignedPower(where(returns < 0 ? stddev(returns, 20) : close), 2.0), 5)) - 0.5"
alpha = calc_str(string, data)
```

Alternatively, Frozen also provides efficient batch processing in case of multiple factors. Here's how to calculate factors simultaneously or by batch:

```python
# define instrument universe
universe = ...
# prepare related data
data_definitions = [
    ("stock_daily_hfq", ("open", "close", "pct_chg", "vol"), ("open", "close", "returns", "volume")),
    ]
data = batchLoader(data_definitions, universe)
# calculate factors
str_list, name_list = [], []
str_list += ["rank(Ts_ArgMax(SignedPower(where(returns < 0 ? stddev(returns, 20) : close), 2.0), 5)) - 0.5"]
name_list += ["alpha001"]
str_list += ["-1 * corr(rank(delta(log(volume), 2)), rank(((close - open) / open)), 6)"]
name_list += ["alpha002"]
str_list += ["-1 * corr(rank(open), rank(volume), 10)"]
name_list += ["alpha003"]
alpha = batch_calc(str_list, name_list, data)
```

_Frozen is also integrated with `qlib` expression engine, based upon which we have straightened the engine's logic and organized the structure. For details of the engine, please refer to [this document](./frozen/factor/extensions/_qlibExpr/README.md)._

### Factor Evaluation

Conduct comprehensive factor performance evaluation through [alphalens](https://github.com/quantopian/alphalens) framework, with detailed methodologies demonstrated in the [factor analysis](./demo/workflow/factor_analysis.ipynb) notebook.

_Note: While the original `alphalens` implementation is limited to matplotlib figure generation, our enhanced version enables automated generation of HTML reports and PNG outputs._

Aside from that, Frozen also provides a rich set of tools with the [plotting](./frozen/utils/plotting.py) module, offering graphical representations and analytical capabilities for factor performance attribution.

### Factor Registration

Register effective factors and save factor values into local database for future application (extraction, combination, etc.).

A typical usage example is as follows:

```python
# register factor
...
# initialize factor base
fb = FactorBase("mongodb")
# save factor
fb.factor2db(factor=alpha, factor_name="alpha001", table_name="factor")
# load factor
alpha = read_factor(table_name="factor", factor_name="alpha001")
```

## Strategy Implementation

**The third step**: Implement trading strategy and perform backtest.

### Portfolio Construction

1. Select targeted instruments based on factor values

```python
# load factor data as score
score = alpha.data
# compute period score by resampling
period_score = score.resample(date_rule, label="right").mean()
# sort ticker scores
ticker_by_order = period_score.loc[date].sort_values(ascending=False).index
# select target instruments
target_instruments = ticker_by_order[:max_instruments]
```

2. Retrieve optimal portfolio weights from risk model

```python
from frozen.engine.riskmodel.optimizer import PortOpt
# initialize risk model
rm = PortOpt()
# retrieve in-sample estimation period
in_sample = ...
# calculate portfolio weights
weights = rm.calc_portfolio_weights(max_instz=max_instruments, X=in_sample)
```

### Trading Signal Generation

Once optimal portfolio weights are determined, the system automatically generates trading signals according to predefined logic at each rebalancing interval. The simulated signals are then transformed into virtual orders, where they're executed through our event-driven backtest engine and converted into assumed positions. This standardized process is looped repeatedly inside the engine, following the strategy logic.

The execution flow inside the backtest engine is only presumptive and ideal, without taking real-world conditions into account. For automated execution in real-time markets, the platform provides synchronized [live trading](#live-trading-1) capabilities that seamlessly translate optimized portfolio weights into executable trading signals. With the quantitative trading services provided by QMT, we can easily place orders and realize various specific needs.

### Performance Assessment

Leverage open-sourced [quantstats](https://github.com/ranaroussi/quantstats) framework for various evaluation of backtesting results.

```python
from frozen.utils import freport, fplot
# Metrics table
table = freport.print_evaluation_table(port_ret, print_table=True)
# Order excel sheet
freport.to_excel(order_history, file_path="./")
# Performance web sheet
freport.to_quantstats(port_ret, file_path="./")
# Performance plot
fplot.create_pnl_plot(port_ret, date_rule, plot_type="all")
```

Additionally, Frozen offers an extensive suite of visualization tools, enabling sophisticated strategy performance analysis and risk assessment through interactive and customizable graphical representations. Detailed results are presented in [graphical reports](./demo/workflow/graphical_report.ipynb).

## Trade Live

Coming soon...

## Customized Research Workflow

The automatic workflow may not suit the research workflow of all Quant researchers. To support a flexible Quant research workflow, Frozen also provides a modularized interface to allow researchers to build their own workflow by code. [Here](./demo/tutorial/workflow_by_code.ipynb) is a demo for customized Quant research workflow by code.

### Minimal Strategy

The following is a minimal Cross-Sectional Factor strategy example that uses bar data. While this platform supports very advanced trading strategies, it is also possible to create simple ones. Start by inheriting from the Strategy base class and implement only the methods required by your strategy.

```python
class FactorFactory(FrozenBt):
  """
  A simple factor-based example strategy.

  When the factor score is in the top 5% of the universe, then enters
  a position at the market in that direction.

  Cancels all orders and closes all positions on period end.
  """

    def __init__(self):
        # Retrieve instrument universe
        self.univ()
        # Configuration
        super().__init__(__file__, self.universe)
        self._calc_flag = False

    def univ(self):
        """
        Define instrument universe on start.
        """
        self.universe = dataloader.load_stock_alternative("stock_list")["ts_code"]

    def prepare_data(self):
        """
        Prepare related data to be used in factor calculation.
        """
        data_definitions = [
            ("stock_daily_hfq", ("close", "pct_chg"), ("close", "returns")),
            ]
        return batchLoader(data_definitions, self.universe)

    def calc(self):
        """
        Calculate and wrap factor data.
        """
        string = "rank(Ts_ArgMax(SignedPower(where(returns < 0 ? stddev(returns, 5) : close), 2.0), 5))"
        alpha = calc_str(string, self.prepare_data())

        self.alpha = alpha
        self._calc_flag = True
```

### Backtest demo

The chart below is produced by a backtest of the Alpha101 factor strategy documented in the [tutorials](./demo/tutorial/) (and available in the [demo repository](./demo/strategy/)). Entry signals are defined by factor scores, with exit targets defined by the rebalancing interval. Stop-losses are automatically placed using the custom PnL threshold, and position sizes are dynamically calculated based on risk model defined in the strategy configuration.

Running this strategy with Frozen in backtest mode will produce the following interactive chart.

<p align="center">
  <img src="./docs/source/_static/img/interactive.gif" />
</p>

Note that strategy portfolio value evolves synchronously with respect to the benchmark. This allows you to see how effective your strategy is - are you outperforming the market baseline? Are your excess returns acceptable and in line with your expectations? Frozen helps you visualize your strategy and answer these questions.

### Run a single factor

All the strategies listed above are runnable with ``Frozen``. Users can find the config files we provide and some details about the model through the [config](./frozen/basis/config/) folder. More information can be retrieved at the [strategy](./demo/strategy/) files listed above.

`Frozen` provides two different ways to run a single factor model, users can pick the one that fits their cases best:

- Users can use the tool `frun` mentioned below to run a strategy's workflow based on the config file.
- Users can create a `workflow_by_code` python script based on the [one](./demo/tutorial/workflow_by_code.ipynb) listed in the `demo` folder.

### Run multiple factors

`Frozen` also provides a script [`run_all_factor.ipynb`](./demo/workflow/run_all_factor.ipynb) which can run multiple factors for several iterations. (**Note**: Currently the script doesn't support parallel running multiple factors, this will be fixed in the future development.)

Here is an example of running all the factors:

```python
# instantiate factory
factory = FactorFactory()
# calculate factors
factory.calc()
# run multiple factors
factory.run_batch(plot_type="line")
```

### Example Strategies

Example strategies can be found in the [demo repository](./demo/strategy).

# Auto Quant Research Workflow

Frozen provides a tool named `frun` to run the whole workflow automatically (including building database, calculating factors, backtest and evaluation). You can start an auto quant research workflow and have a graphical reports analysis according to the following steps:

1. Quant Research Workflow: Run `frun` with strategy name as following.

```bash
   frun strategy1
   # If you would like to customize backtest output
   frun --file_name strategy1 --plot_type all --excel True
```

   If users want to use `frun` under debug mode, please use the following command:

```bash
   python -m pdb frozen/workflow/cli.py strategy1
```

   The result of `frun` is as follows, please refer to [Graphical Evaluation]() for more details about the result.

```bash
   'The following are analysis results of the strategy return with cost.'
   +-----------+-------------+---------------+--------+---------+----------+-----------+---------+------------+----------------+
   |   Account |   Benchmark |   Annual Rate |   Beta |   Alpha |   Sharpe |   Sortino |      IR |   Win Rate |   Max Drawdown |
   +===========+=============+===============+========+=========+==========+===========+=========+============+================+
   |    0.9133 |      0.9462 |       -0.2541 | 0.0327 | -0.2487 |  -1.6011 |   -1.9949 | -0.0315 |      0.475 |         0.1652 |
   +-----------+-------------+---------------+--------+---------+----------+-----------+---------+------------+----------------+
```

   Here are detailed documents for `frun` and [workflow]().

2. Graphical Reports Analysis: Run `demo/workflow/graphical_report.ipynb` with `jupyter notebook` to get graphical reports.

   - Forecasting signal (factor) analysis
     - Cumulative Return of groups
       ![Cumulative Return](./docs/source/_static/img/factor_layer.png)
     - Information Coefficient (IC)
       ![Information Coefficient](./docs/source/_static/img/ic.png)
     - Alpha Decay (Half-life)
       ![Alpha Decay](./docs/source/_static/img/half_life.png)

   For more factor graphical results, please refer to [Factor Evaluation](#factor-evaluation).

   - Portfolio analysis
     - Backtest Return
       ![Backtest Return](./docs/source/_static/img/port_pnl.png)
     - Backtest Return Distribution
       ![Return Distribution by period](./docs/source/_static/img/ret_dist.png)
     - Strategy Return Calendar
       ![Strategy Return Calendar](./docs/source/_static/img/ret_cal.png)
     - Batch Backtest Return
       ![Batch Backtest Return](./docs/source/_static/img/batch_cum_ret.png)

   For more backtest graphical results, please refer to [Performance Assessment](#performance-assessment).

   - [Explanation]() of above results

# Interactive Visualization Tools

## Real-Time Monitor Panel

with Grafana, released in the future

## Web UI Design

with Streamlit, released in the future

## Mobile App Design

with Flutter, released in the future

# More About Frozen

## Pros and Cons

### Our Strengths

- Easy to learn, easy to use, easy to customize.

- Provide a rich set of widgets for performance visualization purposes.

- We dedicate to make the backtest results as accurate as possible, with backtest engine optimized especially for Chinese stock market.

- Modulerized features, each component can be used stand-alone.

- Implementation of risk model to meet clients' specific risk appetite by introducing optimized position weights generated by MPT.

### Our Weaknesses

- Does not support intra-day trading, or any frequency beyond daily

- Designed for cross-sectional factor strategies only. Trend-following and CTA strategies are not supported so far.

- Parameter tuning for both factor and strategy may result in overfitting.

- The whole backtest workflow only attempts to replicate strategy performance in the real world with the utmost effort, but still ignoring much of the details (unforeseeable events, etc.).

- Backtest results only serve as a reference (no more than experimental and far from being trustworthy).

## Documentation

If you want to have a quick glance at the most frequently used components of Frozen, you can try notebooks [here](./demo/tutorial/).

The detailed documents are organized in [docs](./docs/).
[Sphinx](http://www.sphinx-doc.org) and the readthedocs theme is required to build the documentation in html formats.

```bash
cd docs/
conda install sphinx sphinx_rtd_theme -y
# Otherwise, you can install them with pip
# pip install sphinx sphinx_rtd_theme
make html
```

You can also view the [latest document](http://frozen.readthedocs.io/) online directly.

Frozen is in active and continuing development. Our plan is in the roadmap, which is managed as a [markdown file](./ROADMAP.md).

## Related Reports

- [量化回测框架Frozen 1.0](https://mp.weixin.qq.com/s/c46uaRLmoaPaoKiPOACm3A?token=1336521998&lang=zh_CN)
- [量化回测框架Frozen 2.0](https://mp.weixin.qq.com/s/lzyzCN4IBCVRsjCVnOFgfQ?token=1336521998&lang=zh_CN)

# Legal

## License

Frozen is a closed-source project under End-User License Agreement. Please make sure you read all the terms carefully before using Frozen. Detailed imformation about the license can be found [here](./LICENSE).

## Disclaimer

A strategy's past performance does not account for its future returns. Due to the volatile nature of financial market, backtest assumptions may not be accurate, and backtest results are only hypothetical without any guarantee. Therefore, there might be slight difference between backtest results and live trading results.

Never risk money you cannot afford to lose. Always test your strategies on a paper trading account before taking it live.

# Contact us

Contribute to view the source code, to contact us, please scan the QR code below.

- If you have any issues, please create issue [here](https://github.com/Mxyzptlk-Z/frozen/issues) or send messages in WeChat.
- If you want to make contributions to `Frozen`, please [create pull requests](https://github.com/Mxyzptlk-Z/frozen/pulls).
- For other reasons, you are welcome to contact us by email ([ericcccc_z@outlook.com](ericcccc_z@outlook.com)).

Follow us on WeChat:

| [WeChat](https://gitter.im/Microsoft/qlib)                         |
| --------------------------------------------------------------- |
| ![image](./docs/source/_static/img/qrcode.svg) |

<p align="right">[<a href="#-contents----️">back to top</a>]</p>
