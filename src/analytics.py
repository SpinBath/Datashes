def count_unique_hosts(df):
    return df.select("host").distinct().count()

def count_errors_404(df):
    return df.filter(df["status"] == 404).count()

def count_errors(df):
    return df.groupBy(df["status"]).count().orderBy("status")

def top_endpoints(df, n=10):
    return df.groupBy("endpoint").count().orderBy("count", ascending=False).limit(n)

""" def daily_traffic(df):
    return df.groupBy("date").count().orderBy("date") """
