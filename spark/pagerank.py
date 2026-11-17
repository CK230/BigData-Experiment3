import argparse
from operator import add

from pyspark import SparkContext


INITIAL_RANK = 1.0
DAMPING = 0.85
BASE_RANK = 0.15
ITERATIONS = 10


def parse_graph_line(line):
    fields = line.split("\t")
    if len(fields) != 2:
        raise ValueError(f"无效的输入记录：{line!r}")

    page = fields[0].strip()
    if not page or "," in page:
        raise ValueError(f"无效的网页名：{line!r}")

    targets = tuple(target.strip() for target in fields[1].split(","))
    if not all(targets) or len(set(targets)) != len(targets):
        raise ValueError(f"出链为空或重复：{line!r}")
    return page, targets


def validate_graph(links):
    counts = links.mapValues(lambda _: 1).reduceByKey(add).cache()
    try:
        if counts.isEmpty():
            raise ValueError("输入图为空")
        duplicates = counts.filter(lambda entry: entry[1] > 1).keys().take(1)
        if duplicates:
            raise ValueError(f"网页存在多条邻接记录：{duplicates[0]}")

        targets = links.values().flatMap(lambda values: values)
        missing = targets.subtract(counts.keys()).take(1)
        if missing:
            raise ValueError(f"链接目标缺少邻接记录：{missing[0]}")
    finally:
        counts.unpersist()


def distribute(entry):
    _, (targets, rank) = entry
    share = rank / len(targets)
    return ((target, share) for target in targets)


def update_rank(entry):
    _, contribution = entry
    return BASE_RANK + DAMPING * (0.0 if contribution is None else contribution)


def format_result(entry):
    page, rank = entry
    return f"({page},{rank:.10f})"


def main():
    parser = argparse.ArgumentParser(description="Spark RDD PageRank，固定迭代 10 轮")
    parser.add_argument("input", help="输入 TSV 的路径")
    parser.add_argument("output", help="尚未存在的输出目录")
    args = parser.parse_args()

    with SparkContext(appName="Experiment3PageRank") as sc:
        links = sc.textFile(args.input).map(parse_graph_line).cache()
        validate_graph(links)
        ranks = links.mapValues(lambda _: INITIAL_RANK)

        for _ in range(ITERATIONS):
            contributions = links.join(ranks).flatMap(distribute).reduceByKey(add)
            # 从完整网页集合出发，确保零入度网页也保留 0.15 的基础分。
            ranks = links.leftOuterJoin(contributions).mapValues(update_rank)

        ranks.sortByKey(numPartitions=1).map(format_result).saveAsTextFile(args.output)


if __name__ == "__main__":
    main()
