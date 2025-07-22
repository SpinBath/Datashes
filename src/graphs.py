import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd

from src.analytics import top_endpoints, count_unique_hosts, daily_traffic, hourly_activity 

import matplotlib.pyplot as plt

def graph_endpoints(df):

    df = top_endpoints(df).toPandas()

    top_df = df
    plt.figure(figsize=(10, 6))
    sns.barplot(data=top_df, x='count', y='endpoint', palette='Blues_r')
    plt.title(f'Top 20 Endpoints')
    plt.xlabel('Cantidad de llamadas')
    plt.ylabel('Endpoint')
    plt.tight_layout()
    plt.show()


def graph_dailyTraffic(df):
    

    df_hourly = hourly_activity(df).toPandas()

    df_hourly["start"] = df_hourly["window"].apply(lambda w: w["start"].strftime('%H:%M'))

    full_hours = pd.DataFrame({'start': [f"{h:02d}:00" for h in range(24)]})

    df = full_hours.merge(df_hourly[["start", "count"]], on="start", how="left").fillna(0)
    df["count"] = df["count"].astype(int)

    df = df.sort_values("start")

    plt.figure(figsize=(14, 6))
    sns.lineplot(data=df, x="start", y="count", marker="o")

    plt.title(f"Actividad del mes")
    plt.xlabel("Hora del día")
    plt.ylabel("Cantidad de peticiones")
    plt.xticks(rotation=45)
    plt.grid(True, linestyle='--', alpha=0.4)
    plt.tight_layout()
    plt.show()







