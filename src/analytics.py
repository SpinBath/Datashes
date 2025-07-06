from pyspark.sql.functions import hour

# Recuento de pet/host
def count_unique_hosts(df):
    return df.select("host").distinct().count()

# Recuento de errores 404
def count_errors_404(df):
    return df.filter(df["status"] == 404).count()

# Recuento de cod/status
def count_errors(df):
    return df.groupBy(df["status"]).count().orderBy("status")

# Rutas mas solicitadas

def top_endpoints(df, n=10):
    return df.groupBy("endpoint").count().orderBy("count", ascending=False).limit(n)

# Trafico diario

def daily_traffic(df):
    return df.groupBy("date").count().orderBy("date")

# Actividad por horas

def hourly_activity(df):
    return df.groupBy(hour("timestamp").alias("hour")).count().orderBy("hour")

# Accesos a rutas inexistentes

def inex_routes(df):
    return df.filter(df["status"] == 404).orderBy("endpoint")




