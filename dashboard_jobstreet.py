import streamlit as st
import matplotlib.pyplot as plt
import plotly.express as px
import altair as alt
import pydeck as pdk
import pandas as pd
import numpy as np

st.set_page_config(
    page_title="ELT Project - Jobstreet",
    layout="wide",
    initial_sidebar_state="expanded"
)

####################### LOAD DATA #######################
data = pd.read_csv("C:/Users/valin/airflow-docker/output/data_jobstreet.csv")

st.markdown("<h1 style='text-align: center;'>💼 ELT Project: Jobstreet Data Pipeline</h1>", unsafe_allow_html=True)
st.markdown("### 🎯 Goal")
st.markdown("""
Want to know what **information and insights** can be gathered from job vacancy data on Jobstreet.
""")
st.divider()

####################### ELT STEP #######################
st.markdown("### 🔄 ELT Process (Extract - Load - Transform)")
st.columns(3)[0].markdown("#### 📥 Extract\nFetching data from Jobstreet using Airflow & Selenium.")
st.columns(3)[1].markdown("#### 🗃️ Load\nStoring scraping results data to Impala (Data Lake).")
st.columns(3)[2].markdown("#### 🧪 Transform\nPerforming data transformation in Impala.")
st.divider()

####################### ELT PIPELINE #######################
st.markdown("### ⚙️ ELT Pipeline")
st.image("ELT_Scraping_Jobstreet.drawio.png", caption="ELT Pipeline", use_container_width=True)
st.divider()

st.markdown("### ⚙️ ETL Pipeline: Explanation")

# kolom
col1, col2, col3, col4 = st.columns(4)

with col1:
    if st.button("🟢 Extract", use_container_width=True):
        st.markdown("### Extract Data")
        st.markdown("""
        - Scrape job listings from Jobstreet
        - Using Selenium
        - The scraped data includes: `job_titles`, `posting_date`, `location`, `type_work`, `classification`, `company`,  `salary`
        - The results are saved to a file called `scraped_data.csv`
        """)

with col2:
    if st.button("📥 Load", use_container_width=True):
        st.markdown("### Load Data")
        st.markdown("""
        - Read `scraped_data.csv`
        - Load the scraped job listing data into Impala, specifically into the temporary table: `tmp_result_scrape_jobstreet`
        """)

with col3:
    if st.button("🧪 Transform", use_container_width=True):
        st.markdown("### Transform Data")
        st.markdown("""
        - Standardize the date format
        - Split the location field into province and city
        - Extract minimum, maximum, and average salary
        - Extract classification
        - Remove duplicate data
        - Join with latitude and longitude coordinates
        - Save the cleaned data to the final table: `result_scrape_jobstreet`
        """)

with col4:
    if st.button("📊 Visualize", use_container_width=True):
        st.markdown("### Visualization")
        st.markdown("""
        - The transformed data is visualized through an interactive Streamlit dashboard
        - Users can filter job listings based on:
            - Province
            - Job classification (e.g., IT, Finance, etc.)
        - Several visualizations are included to analyze job trends, company insights, salary distributions, and more
        """)

st.divider()

####################### DETAIL AIRFLOW #######################
st.markdown("### 🧩 Details of the Airflow Process")
st.image("Detail Airflow.png", caption="Airflow Process", use_container_width=True)
st.divider()

####################### DATA PREVIEW #######################
st.markdown("### 📊 Preview Data Jobstreet")
st.markdown("Example of data from scraping and transformation:")
st.dataframe(data)
st.divider()

####################### SIDEBAR #######################

st.sidebar.image("D:\Pictures\streamlit-logo.png", use_container_width=True)

with st.sidebar.expander("📍 Filter Provinsi", expanded=False):
    select_all = st.checkbox("Select All", value=True, key="select_all_prov")
    reset_filter = st.button("🔄 Reset Filter")

    provinsi_list = sorted(data['prov'].dropna().unique())

    if select_all:
        selected_provinces = st.multiselect(
            "Pilih Provinsi", options=provinsi_list, default=provinsi_list, key="provinsi_select"
        )
    else:
        selected_provinces = st.multiselect(
            "Pilih Provinsi", options=provinsi_list, key="provinsi_select"
        )

    # Tombol reset
    if reset_filter:
        selected_provinces = provinsi_list
        st.experimental_rerun()

with st.sidebar.expander("🏷️ Filter Classification", expanded=False):
    select_all_class = st.checkbox("Select All", value=True, key="select_all_class")
    reset_class_filter = st.button("🔄 Reset Classification")

    classification_list = sorted(data['classification'].dropna().unique())

    if select_all_class:
        selected_classes = st.multiselect(
            "Pilih Classification", options=classification_list, default=classification_list, key="class_select"
        )
    else:
        selected_classes = st.multiselect(
            "Pilih Classification", options=classification_list, key="class_select"
        )

    if reset_class_filter:
        selected_classes = classification_list
        st.experimental_rerun()

filtered_data = data[data['prov'].isin(selected_provinces)]
filtered_data = filtered_data[filtered_data['classification'].isin(selected_classes)]
if filtered_data.empty:
    st.warning("There is no data for the selected province.")
    st.stop()

# range salary data
min_salary = int(filtered_data['avg_salary'].min())
max_salary = int(filtered_data['avg_salary'].max())

# slider di sidebar
salary_range = st.sidebar.slider(
    "Select Salary Range (IDR)",
    value=(min_salary, max_salary),
    step=1000000 
)

filtered_data = filtered_data[
    (filtered_data['avg_salary'] >= salary_range[0]) &
    (filtered_data['avg_salary'] <= salary_range[1])
]

with st.sidebar:
    st.markdown("---")
    st.markdown("### 👩‍💻 Created by")
    st.markdown("**Nabila Valinka**")
    st.markdown(
        """
        Visit my profile:
        
        [🔗 LinkedIn](https://www.linkedin.com/in/nabilavlnk)  
        [💻 GitHub](https://github.com/nabilavlnk)
        """
    )
    st.markdown("Thank you for using this dashboard! 🙏")

####################### DASHBOARD #######################
st.markdown("### 📈 Visualization Dashboard")

filtered_data['avg_salary'] = (
    filtered_data['avg_salary']
    .astype(str)           # pastikan string
    .str.replace(',', '')  # buang koma
    .astype(float)         # ubah ke float
)

# Hitung unique
total_job_titles = filtered_data['job_titles'].count()
total_companies = filtered_data['company'].nunique()
min_salary = filtered_data['min_salary'].min()
max_salary = filtered_data['max_salary'].max()
avg_salary = filtered_data['avg_salary'].mean()

####### METRICS
col1, col2, col3, col4, col5 = st.columns([1, 1, 1.5, 1.5, 1.5])

with col1:
    st.metric("Total Job Titles", total_job_titles)

with col2:
    st.metric("Total Companies", total_companies)

with col3:
    st.metric("Minimum Salary", f"Rp {min_salary:,.0f}")

with col4:
    st.metric("Average Salary", f"Rp {avg_salary:,.0f}")

with col5:
    st.metric("Maximum Salary", f"Rp {max_salary:,.0f}")

####### MAP
df_job_count = filtered_data.groupby('prov').size().reset_index(name='job_count')
df_latlong = filtered_data[['prov', 'lat', 'long']].drop_duplicates()
df_map = df_job_count.merge(df_latlong, on='prov', how='left')

layer = pdk.Layer(
    "ScatterplotLayer",
    data=df_map,
    get_position='[long, lat]',
    get_radius='job_count * 2000',
    get_fill_color='[255, 0, 0, 140]',
    pickable=True
)

view_state = pdk.ViewState(
    latitude=df_map['lat'].mean(),
    longitude=df_map['long'].mean(),
    zoom=4
)

####### LINE CHART POSTING PER TANGGAL DAN JOB TITLE
# datetime
job_count = filtered_data.groupby(['posting_date']).size().reset_index(name='total_postings')
job_count['posting_date'] = pd.to_datetime(job_count['posting_date'])
job_count['posting_date_str'] = job_count['posting_date'].dt.strftime('%d-%m-%Y')

# Line Chart (pakai datetime di X)
line_chart = alt.Chart(job_count).mark_line().encode(
    x=alt.X('posting_date:T', 
            title='Date',
            axis=alt.Axis(format='%d-%m-%Y', labelAngle=-90)
            ),
    y=alt.Y('total_postings:Q', 
            title='Total of Job Vacancies',
            scale=alt.Scale(domainMin=0, nice=False),
            axis=alt.Axis(tickMinStep=1, tickCount=len(job_count['total_postings'].unique()), format='d')
            ),
    tooltip=[
        alt.Tooltip('posting_date_str:N', title='Date'),
        alt.Tooltip('total_postings:Q', title='Total of Job Vacancies')
    ]
)

# Max point
max_row = job_count.loc[job_count['total_postings'].idxmax()]
max_point_df = pd.DataFrame([max_row])
max_point_df['posting_date'] = pd.to_datetime(max_point_df['posting_date'])
max_point_df['posting_date_str'] = max_point_df['posting_date'].dt.strftime('%d-%m-%Y')

max_point = alt.Chart(max_point_df).mark_point(
    color='red', size=100, shape='triangle'
).encode(
    x='posting_date:T',
    y='total_postings:Q',
    tooltip=[
        alt.Tooltip('posting_date_str:N', title='Tanggal Max'),
        alt.Tooltip('total_postings:Q', title='Max Postingan')
    ]
)

# Label MAX
max_label = alt.Chart(max_point_df).mark_text(
    align='left',
    dx=5,
    dy=-5,
    fontSize=13,
    fontWeight='bold',
    color='red'
).encode(
    x='posting_date:T',
    y='total_postings:Q',
    text=alt.value('MAX')
)

# Final chart
final_chart = (line_chart + max_point + max_label).properties(
    width=700,
    height=400,
    title='Job Posts by Date'
)

####### KOLOM MAP & LINE CHART

col_map, col_line = st.columns([2, 2]) 

with col_map:
    st.subheader("Job Vacancies by Province")
    st.pydeck_chart(pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        tooltip={"text": "{prov}\n{job_count} postings"}
    ), use_container_width=True, height=400)

with col_line:
    st.subheader("Job Postings Trend by Date")
    st.altair_chart(final_chart, use_container_width=True)

####### LINE CHART DATE & CLASS
filtered_data['posting_date'] = pd.to_datetime(filtered_data['posting_date'])

grouped = filtered_data.groupby(['posting_date', 'classification']).size().reset_index(name='count')
pivot_df = grouped.pivot(index='posting_date', columns='classification', values='count').fillna(0)
pivot_df = pivot_df.sort_index()
top_classifications = pivot_df.columns
filtered_df = pivot_df[top_classifications]

st.subheader("Job Vacancy Trends by Classification")

# Plotly 
fig = px.line(
    filtered_df,
    x=filtered_df.index,
    y=filtered_df.columns,
    labels={'value': 'Total of Job Vacancies', 
            'variable': 'Classification', 
            'posting_date': 'Date'},
    title=f'Trend of Job Classifications'
)

fig.update_yaxes(
    tickmode='linear',
    dtick=1,
    tickformat=',d'
)

st.plotly_chart(fig, use_container_width=True)

####### PIE CHART TYPE WORK
type_counts = filtered_data['type_work'].value_counts().reset_index()
type_counts.columns = ['type_work', 'count']
type_counts['percent'] = (type_counts['count'] / type_counts['count'].sum() * 100).round(2)

pie = alt.Chart(type_counts).mark_arc().encode(
    theta=alt.Theta(field="count", type="quantitative"),
    color=alt.Color(field="type_work", type="nominal"),
    tooltip=['type_work', 'count', 'percent']
).properties(
    width=700,
    height=400
)

####### BAR CHART JOB_TITLES PER CLASSIFICATION
classification_counts = filtered_data.groupby('classification')['job_titles'].count().reset_index()
classification_counts = classification_counts.sort_values(by='job_titles', ascending=False)

bar_class = alt.Chart(classification_counts).mark_bar().encode(
    x=alt.X('classification:N', 
            sort=alt.EncodingSortField(field='job_titles', order='descending'),
            title='Classification'),
    y=alt.Y('job_titles:Q', title='Job Vacancies'),
    tooltip=['classification', 'job_titles']
).properties(
    width=700,
    height=400
)

####### BAR CHART JOB_TITLES PER COMPANY
company_counts = filtered_data.groupby('company').size().reset_index(name='job_count')
print(company_counts.dtypes)
print(company_counts.head())
company_counts = company_counts.sort_values(by='job_count', ascending=False)

bar_comp = alt.Chart(company_counts).mark_bar().encode(
    x=alt.X('company:N',
            sort=alt.SortField(field='job_count', order='descending'),
            title='Company'),
    y=alt.Y('job_count:Q', title='Job Vacancies', axis=alt.Axis(format='d')),
    tooltip=['company', 'job_count']
).properties(
    width=700,
    height=400
)

####### KOLOM PIE, BAR, BAR
col_type, col_class, col_comp = st.columns([2,2,2])

with col_type:
    st.subheader("Comparison of Type Work")
    st.altair_chart(pie, use_container_width=True)

with col_class:
    st.subheader("Total of Vacancies by Classification")
    st.altair_chart(bar_class, use_container_width=True)

with col_comp:
    st.subheader("Total of Vacancies per Company")
    st.altair_chart(bar_comp, use_container_width=True)

####### BAR CHART PROV & CLASS
prov_class = filtered_data.groupby(['prov', 'classification'])['job_titles'].count().reset_index()
bar_prov_class = alt.Chart(prov_class).mark_bar().encode(
    x=alt.X('prov:N', title='Provinsi'),
    y=alt.Y('job_titles:Q', title='Jumlah Lowongan'),
    color='classification:N',
    tooltip=['prov', 'classification', 'job_titles']
).properties(
    width=1000,
    height=400
)
st.subheader("Total of Job Vacancies by Province and Classification")
st.altair_chart(bar_prov_class, use_container_width=True)

####### TABLE TOP COMPANY
top_company_salary = (
    filtered_data.groupby('company')
    .agg(avg_salary=('avg_salary', 'mean'), job_count=('job_titles', 'count'))
    .reset_index()
    .sort_values(by='avg_salary', ascending=False)
    .head(5)
)
top_company_salary = top_company_salary[['company', 'avg_salary']]

####### TABLE TOP PROVINCE
top_province_salary = (
    filtered_data.groupby('prov')
    .agg(avg_salary=('avg_salary', 'mean'), job_count=('job_titles', 'count'))
    .reset_index()
    .sort_values(by='avg_salary', ascending=False)
    .head(5)
)
top_province_salary = top_province_salary[['prov', 'avg_salary']]

####### TABLE TOP CLASS
top_class_salary = (
    filtered_data.groupby('classification')
    .agg(avg_salary=('avg_salary', 'mean'), job_count=('job_titles', 'count'))
    .reset_index()
    .sort_values(by='avg_salary', ascending=False)
    .head(5)
)
top_class_salary = top_class_salary[['classification', 'avg_salary']]

####### KOLOM 3 TOP TABLE
col_top_comp, col_top_sal, col_top_class = st.columns([2,2,2])

with col_top_comp:
    st.subheader("Top 5 Highest-Paying Companies")
    st.dataframe(top_company_salary, hide_index=True, use_container_width=True)

with col_top_sal:
    st.subheader("Top 5 Highest-Paying Province")
    st.dataframe(top_province_salary, hide_index=True, use_container_width=True)

with col_top_class:
    st.subheader("Top 5 Highest-Paying Class")
    st.dataframe(top_class_salary, hide_index=True, use_container_width=True)

st.divider()