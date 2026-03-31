"""
Dashboard FAO Food Price Index — filtros, tendências e EDA (Pandas + Plotly + Matplotlib).

Execute a partir da pasta webscraping_agro:
  streamlit run streamlit_app.py
"""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import plotly.express as px
import streamlit as st

from src.analysis.eda_tools import descriptive_price_stats, mark_outliers_iqr

APP_ROOT = Path(__file__).resolve().parent
PROCESSED_GLOB = APP_ROOT / "data" / "processed" / "fao" / "food_price_index"


def find_latest_processed_parquet(base: Path = PROCESSED_GLOB) -> Path:
    candidates = sorted(base.glob("ingested_at=*/run_id=*/prices_processed.parquet"))
    if not candidates:
        raise FileNotFoundError(
            f"Nenhum prices_processed.parquet em {base}. Rode: python -m src.etl.run_etl"
        )
    return candidates[-1]


@st.cache_data(show_spinner=False)
def load_prices(parquet_path: str) -> pd.DataFrame:
    path = Path(parquet_path)
    df = pd.read_parquet(path)
    df["price_month"] = pd.to_datetime(df["price_month"])
    return df


def main() -> None:
    st.set_page_config(page_title="FAO — Preços e tendências", layout="wide")
    st.title("Índice de preços de alimentos (FAO)")
    st.caption("Fonte processada localmente (camada `processed`). Unidade: pontos de índice.")

    try:
        latest = find_latest_processed_parquet()
    except FileNotFoundError as exc:
        st.error(str(exc))
        st.stop()

    df = load_prices(str(latest))
    st.sidebar.markdown("**Arquivo**")
    st.sidebar.code(latest.relative_to(APP_ROOT).as_posix(), language=None)

    commodities = sorted(df["commodity_code"].unique())
    regions = sorted(df["region_code"].unique())
    min_year = int(df["price_month"].dt.year.min())
    max_year = int(df["price_month"].dt.year.max())

    sel_commodities = st.sidebar.multiselect("Produto (commodity)", commodities, default=commodities)
    sel_regions = st.sidebar.multiselect("Região", regions, default=regions)
    c1, c2 = st.sidebar.columns(2)
    year_from = c1.number_input("Ano inicial", min_value=min_year, max_value=max_year, value=min_year)
    year_to = c2.number_input("Ano final", min_value=min_year, max_value=max_year, value=max_year)
    if year_from > year_to:
        st.sidebar.warning("Ajuste: ano inicial ≤ ano final.")
        st.stop()

    mask = (
        df["commodity_code"].isin(sel_commodities)
        & df["region_code"].isin(sel_regions)
        & (df["price_month"].dt.year >= year_from)
        & (df["price_month"].dt.year <= year_to)
    )
    filtered = df.loc[mask].copy()

    if filtered.empty:
        st.warning("Nenhum registro com os filtros selecionados.")
        st.stop()

    tab_dash, tab_eda = st.tabs(["Dashboard", "Análise exploratória"])

    with tab_dash:
        st.subheader("Tendências e comparativos")
        col_a, col_b, col_c = st.columns(3)
        col_a.metric("Registros (filtro)", f"{len(filtered):,}")
        col_b.metric("Período mín.", filtered["price_month"].min().strftime("%Y-%m"))
        col_c.metric("Período máx.", filtered["price_month"].max().strftime("%Y-%m"))

        fig_line = px.line(
            filtered.sort_values(["commodity_code", "price_month"]),
            x="price_month",
            y="price_value",
            color="commodity_code",
            markers=False,
            title="Série temporal por produto",
        )
        fig_line.update_layout(hovermode="x unified", legend_title_text="Produto")
        st.plotly_chart(fig_line, use_container_width=True)

        agg_mean = filtered.groupby("commodity_code", as_index=False)["price_value"].mean().sort_values(
            "price_value", ascending=False
        )
        fig_bar = px.bar(
            agg_mean,
            x="commodity_code",
            y="price_value",
            title="Preço médio no período filtrado (pontos de índice)",
        )
        st.plotly_chart(fig_bar, use_container_width=True)

        fig_box = px.box(
            filtered,
            x="commodity_code",
            y="price_value",
            color="commodity_code",
            title="Distribuição de preços por produto (boxplot)",
        )
        fig_box.update_layout(showlegend=False)
        st.plotly_chart(fig_box, use_container_width=True)

    with tab_eda:
        st.subheader("Estatísticas descritivas e outliers")
        by_commodity = filtered.groupby("commodity_code")["price_value"].agg(
            ["count", "mean", "median", "std", "min", "max"]
        )
        st.dataframe(by_commodity.style.format("{:.2f}", subset=["mean", "median", "std", "min", "max"]))

        overall = descriptive_price_stats(filtered)
        st.markdown("**Conjunto filtrado (global)**")
        st.json(overall.to_dict())

        flagged = mark_outliers_iqr(filtered, "commodity_code", "price_value")
        outliers = flagged[flagged["is_outlier"]].sort_values(["commodity_code", "price_month"])
        st.markdown(f"**Outliers (IQR por produto): {len(outliers)}**")
        if not outliers.empty:
            st.dataframe(
                outliers[
                    ["commodity_code", "region_code", "price_month", "price_value", "currency_code"]
                ].head(200)
            )
        else:
            st.info("Nenhum outlier detectado com a regra IQR nos dados filtrados.")

        st.markdown("**Gráficos (Pandas / Matplotlib)**")
        mc1, mc2 = st.columns(2)
        with mc1:
            fig_m1, ax1 = plt.subplots(figsize=(8, 4))
            groups = [sub["price_value"].values for _, sub in filtered.groupby("commodity_code")]
            labels = list(filtered.groupby("commodity_code").groups.keys())
            ax1.boxplot(groups, tick_labels=labels, vert=True)
            ax1.set_ylabel("price_value")
            ax1.set_title("Boxplot por produto")
            ax1.tick_params(axis="x", rotation=45)
            fig_m1.tight_layout()
            st.pyplot(fig_m1)
            plt.close(fig_m1)

        with mc2:
            fig_m2, ax2 = plt.subplots(figsize=(8, 4))
            ax2.hist(filtered["price_value"], bins=40, edgecolor="black", alpha=0.85)
            ax2.set_xlabel("price_value")
            ax2.set_ylabel("Frequência")
            ax2.set_title("Histograma dos preços (filtro atual)")
            fig_m2.tight_layout()
            st.pyplot(fig_m2)
            plt.close(fig_m2)

        fig_m3, ax3 = plt.subplots(figsize=(10, 4))
        for code, sub in filtered.groupby("commodity_code"):
            x_ord = sub["price_month"].map(pd.Timestamp.toordinal)
            ax3.scatter(x_ord, sub["price_value"], label=code, alpha=0.65, s=12)
        ax3.set_xlabel("Tempo (ordinal)")
        ax3.set_ylabel("price_value")
        ax3.set_title("Dispersão: tempo × preço por produto")
        ax3.legend(bbox_to_anchor=(1.02, 1), loc="upper left", fontsize=8)
        fig_m3.tight_layout()
        st.pyplot(fig_m3)
        plt.close(fig_m3)


if __name__ == "__main__":
    main()
