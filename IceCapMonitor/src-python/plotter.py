import subprocess
import plotly.express as px
import pandas as pd
import re
from datetime import datetime

OUTPUT_FILES = "/IceCapMonitor/output/*"
CATEGORIES = {
    0: "No Snow/Ice",
    1: "Low Snow/Ice Coverage",
    2: "Moderate Snow/Ice Coverage",
    3: "High Snow/Ice Coverage",
    4: "Very High Snow/Ice Coverage"
}
LANDSAT_RE = re.compile(r"L\w\d{2}_\w{4}_\w{6}_(?P<date>\d{8})_\d{8}_\d{2}_\w{2}.tar")

def read_from_hdfs() -> pd.DataFrame:
    cat = subprocess.Popen(["hadoop", "fs", "-cat", OUTPUT_FILES], stdout=subprocess.PIPE)
    data = {"file": [], "category": [], "count": [], "date": []}

    for line in cat.stdout:
        file, category, count = re.split(r"[,\t]", line.decode().strip())
        landsat = LANDSAT_RE.match(file)
        date = datetime.strptime(landsat["date"], "%Y%m%d").date().isoformat()
        data["file"].append(file)
        data["category"].append(CATEGORIES.get(int(category)))
        data["count"].append(int(count) * (30 * 30) * 0.001)
        data["date"].append(date)
    
    
    return pd.DataFrame(data)

if __name__ == "__main__":
    df = read_from_hdfs()

    fig = px.bar(df, x="date", y="count", color="category", title="NDSI Values from Remote Region of Iceland", labels={"category": "NDSI Category"}, color_discrete_sequence=["#000000", "#333333", "#1a2653", "#006bce", "#b4cffa"])
    fig.update_layout(xaxis_type='category', xaxis_title="Aquisition Date", yaxis_title="Area (km<sup>2</sup>)")
    fig.write_image("out.png", scale=6)

