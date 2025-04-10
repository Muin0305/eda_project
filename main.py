import streamlit as st
import duckdb
import pandas as pd
import os
import plotly.express as px

st.set_page_config(layout="wide", page_title="Customer base analysis")

st.markdown("## Customer base analysis")

# Проверка файла
if not os.path.exists("my.db"):
    st.error("Файл my.db не найден!")
    st.stop()

# Загрузка данных
@st.cache_data
def load_data():
    try:
        with duckdb.connect("my.db", read_only=True) as conn:
            customer_df = conn.execute("SELECT * FROM customer_data").fetchdf()
            sales_df = conn.execute("SELECT DISTINCT customer_key, mall_name FROM sales_data").fetchdf()
            df = customer_df.merge(sales_df, on="customer_key", how="left")
            return df
    except Exception as e:
        st.error(f"Ошибка загрузки: {str(e)}")
        return pd.DataFrame()

df = load_data()

if df.empty:
    st.warning("Нет данных для отображения!")
    st.stop()

# Сайдбар фильтры
with st.sidebar:
    st.header("Mall Customers")
    st.markdown("### Main filter")

    report_date = st.date_input("Select report date", value=pd.to_datetime("2004-07-31"))

    # Пол
    gender_options = ["All"] + df["gender"].dropna().unique().tolist()
    selected_gender = st.selectbox("Select gender", gender_options)

    # Тип оплаты
    payment_options = ["All"] + df["payment_type"].dropna().unique().tolist()
    selected_payment = st.selectbox("Select payment type", payment_options)

    # ТЦ
    mall_options = ["All"] + df["mall_name"].dropna().unique().tolist()
    selected_mall = st.selectbox("Select mall", mall_options)

    # Возраст
    min_age = int(df["customer_age"].min())
    max_age = int(df["customer_age"].max())
    age_range = st.slider("Select age range", min_value=min_age, max_value=max_age, value=(min_age, max_age))

# Применение фильтров
if selected_gender != "All":
    df = df[df["gender"] == selected_gender]

if selected_payment != "All":
    df = df[df["payment_type"] == selected_payment]

if selected_mall != "All":
    df = df[df["mall_name"] == selected_mall]

df = df[(df["customer_age"] >= age_range[0]) & (df["customer_age"] <= age_range[1])]

# Метрики
col1, col2, col3 = st.columns(3)
col1.metric("Number of customers", len(df))
col2.metric("Average customer age", int(df["customer_age"].mean()))
col3.metric("Median customer age", int(df["customer_age"].median()))

# Диаграммы
col4, col5 = st.columns(2)

with col4:
    st.subheader("Customers breakdown by gender")
    gender_chart = px.pie(df, names="gender", hole=0.0)
    st.plotly_chart(gender_chart, use_container_width=True)

with col5:
    st.subheader("Customers breakdown by payment type")
    payment_chart = px.pie(df, names="payment_type", hole=0.5)
    st.plotly_chart(payment_chart, use_container_width=True)

# Гистограмма возраста
st.subheader("Customer ages distribution")
age_hist = px.histogram(df, x="customer_age", nbins=20)
st.plotly_chart(age_hist, use_container_width=True)

# 📊 Количество клиентов по торговым центрам (mall_name)
st.subheader("Customers per mall")

mall_counts = df["mall_name"].value_counts().reset_index()
mall_counts.columns = ["mall_name", "count"]

mall_bar = px.bar(
    mall_counts,
    x="mall_name",
    y="count",
    labels={"mall_name": "Mall name", "count": "Number of customers"},
    color_discrete_sequence=["#636EFA"]
)

st.plotly_chart(mall_bar, use_container_width=True)


# 📈 Тип оплаты по полу (stacked bar)
st.subheader("Payment types by gender")
stacked_df = df.groupby(["gender", "payment_type"]).size().reset_index(name="count")
payment_gender_chart = px.bar(
    stacked_df,
    x="gender",
    y="count",
    color="payment_type",
    barmode="stack",
    title="Payment distribution by gender"
)
st.plotly_chart(payment_gender_chart, use_container_width=True)
