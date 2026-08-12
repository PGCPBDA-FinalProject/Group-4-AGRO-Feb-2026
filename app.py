#!/usr/bin/env python3
"""
==============================================================================
AGROSENSE AI PLATFORM - MINIMALIST PRECISION AGRO-CLIMATIC & RAG ENGINE
==============================================================================
Description: High-performance Agricultural Intelligence Web Application.
             Features 55-Crop Matcher, Plotly Radar & Bar Analytics, RAG AI
             Agronomist, State Soil Explorer, and Micro-Climate & Soil Deck.
==============================================================================
"""

import sys
import os
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from rag_app.crop_engine import QuantitativeCropEngine, RecencyWeightedClimateCalibrator
from rag_app.rag_chain import AgroRAGAdvisoryChain

# ==============================================================================
# STREAMLIT PAGE CONFIGURATION & CUSTOM MINIMALIST THEMING
# ==============================================================================
st.set_page_config(
    page_title="AgroSense AI - Precision Crop & Soil Intelligence",
    page_icon="🌱",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom Minimalist CSS
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800&display=swap');
    
    html, body, [class*="css"] {
        font-family: 'Plus Jakarta Sans', -apple-system, BlinkMacSystemFont, sans-serif;
        color: #0F172A;
    }
    
    /* Clean Minimal Header */
    .minimal-header {
        padding: 0.2rem 0 0.8rem 0;
        margin-bottom: 1.2rem;
        border-bottom: 1px solid #E2E8F0;
    }
    .minimal-title {
        font-size: 1.85rem;
        font-weight: 800;
        letter-spacing: -0.02em;
        color: #0F172A;
        margin: 0;
    }
    .minimal-subtitle {
        font-size: 0.95rem;
        color: #64748B;
        margin-top: 0.2rem;
        font-weight: 400;
    }

    /* Minimal Metric Tiles */
    .metric-tile {
        background: #FFFFFF;
        padding: 1rem 1.1rem;
        border-radius: 12px;
        border: 1px solid #E2E8F0;
        box-shadow: 0 1px 3px rgba(0, 0, 0, 0.02);
        transition: border-color 0.2s ease;
    }
    .metric-tile:hover {
        border-color: #CBD5E1;
    }
    .metric-tile-label {
        font-size: 0.75rem;
        color: #64748B;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    .metric-tile-value {
        font-size: 1.3rem;
        color: #0F172A;
        font-weight: 700;
        margin-top: 0.2rem;
        letter-spacing: -0.01em;
    }
    .metric-tile-sub {
        font-size: 0.75rem;
        color: #10B981;
        font-weight: 600;
        margin-top: 0.15rem;
    }

    /* Micro-Climate & Soil Control Deck Card */
    .input-deck-card {
        background: #F8FAFC;
        border: 1px solid #E2E8F0;
        border-radius: 14px;
        padding: 1.2rem 1.4rem;
        margin-bottom: 1.5rem;
    }

    /* Top Crop Recommendation Minimal Card */
    .recommendation-card {
        background: #FFFFFF;
        border: 1px solid #10B981;
        border-radius: 14px;
        padding: 1.4rem;
        box-shadow: 0 4px 12px -2px rgba(16, 185, 129, 0.08);
    }
    .recommendation-rank {
        font-size: 0.75rem;
        font-weight: 700;
        color: #047857;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    .recommendation-crop {
        font-size: 1.8rem;
        font-weight: 800;
        color: #0F172A;
        margin: 0.2rem 0;
    }
    .recommendation-score {
        font-size: 1.1rem;
        font-weight: 700;
        color: #10B981;
    }

    /* Custom Streamlit Tab Styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
        border-bottom: 1px solid #E2E8F0;
    }
    .stTabs [data-baseweb="tab"] {
        padding: 8px 16px;
        font-size: 0.9rem;
        font-weight: 600;
        color: #64748B;
        border-radius: 8px 8px 0 0;
    }
    .stTabs [aria-selected="true"] {
        color: #0F172A !important;
        border-bottom: 2px solid #10B981 !important;
        background-color: transparent !important;
    }
    </style>
""", unsafe_allow_html=True)


@st.cache_resource
def load_engines():
    """Cache engine initialization for fast web re-renders."""
    gold_s3_bucket = "s3://krishna-agro-gold/dim_crop_agronomy_profile"
    engine = QuantitativeCropEngine(gold_s3_uri=gold_s3_bucket)
    rag_chain = AgroRAGAdvisoryChain()
    return engine, rag_chain


@st.cache_data
def load_dataset_cities():
    """Loads state, district, and city mappings from dataset CSVs."""
    candidates = [
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "tempdata", "weather_unique_cities.csv"),
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "tempdata", "weather_clustered_cities.csv"),
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "tempdata", "weather.csv")
    ]
    for path in candidates:
        if os.path.exists(path):
            try:
                df = pd.read_csv(path)
                df.columns = [c.strip().lower() for c in df.columns]
                if "state" in df.columns and "city" in df.columns:
                    df = df.dropna(subset=["state", "city"])
                    df["state"] = df["state"].astype(str).str.strip()
                    df["city"] = df["city"].astype(str).str.strip()
                    if "district" in df.columns:
                        df["district"] = df["district"].astype(str).str.strip()
                        df["district"] = df["district"].replace("", np.nan).fillna(df["city"])
                    else:
                        df["district"] = df["city"]

                    if "region" in df.columns:
                        df["region"] = df["region"].astype(str).str.strip()
                    else:
                        df["region"] = "General"

                    return df
            except Exception:
                pass
    return None


crop_engine, rag_chain = load_engines()

# ==============================================================================
# SIDEBAR CONTROLS
# ==============================================================================
st.sidebar.markdown("### 🌱 **AgroSense AI**")
st.sidebar.markdown("#### 🗺️ **Location & Cultivation Season**")

# Dynamic Location Selector (State -> District -> City)
df_locations = load_dataset_cities()

if df_locations is not None and not df_locations.empty:
    available_states = sorted(df_locations["state"].unique().tolist())
    default_state_idx = available_states.index("Madhya Pradesh") if "Madhya Pradesh" in available_states else 0
    state = st.sidebar.selectbox("State", available_states, index=default_state_idx)

    df_state = df_locations[df_locations["state"] == state]
    available_districts = sorted(df_state["district"].unique().tolist())
    default_district_idx = available_districts.index("Indore") if "Indore" in available_districts else 0
    district = st.sidebar.selectbox("District", available_districts, index=default_district_idx)

    df_district = df_state[df_state["district"] == district]
    available_cities = sorted(df_district["city"].unique().tolist())
    default_city_idx = available_cities.index("Indore") if "Indore" in available_cities else 0
    city = st.sidebar.selectbox("City / Town", available_cities, index=default_city_idx)

    selected_row = df_district[df_district["city"] == city]
    if not selected_row.empty:
        region = selected_row.iloc[0].get("region", "Central")
        lat = selected_row.iloc[0].get("lat", 22.7167)
        lng = selected_row.iloc[0].get("lng", 75.8472)
    else:
        region, lat, lng = "Central", 22.7167, 75.8472
else:
    state = st.sidebar.selectbox("State", [
        "Madhya Pradesh", "Maharashtra", "Uttar Pradesh", "Punjab", "Haryana",
        "Rajasthan", "Gujarat", "Chhattisgarh", "Tamil Nadu", "Karnataka",
        "West Bengal", "Bihar", "Odisha", "Kerala", "Assam"
    ])
    district = st.sidebar.text_input("District", value="Indore")
    city = st.sidebar.text_input("City / Town", value="Indore")
    region, lat, lng = "Central", 22.7167, 75.8472

season = st.sidebar.selectbox("Cultivation Season", [
    "Monsoon (Kharif)", "Winter (Rabi)", "Summer (Zaid)"
])

use_recency = st.sidebar.checkbox(
    "🕒 Recency-Weighted Climate Calibration",
    value=True,
    help="Applies Exponential Time-Decay Weighting (decay=0.85), giving 75%+ total importance weight to recent 5-year climate trends (2020-2025) over older historical decades."
)

# Auto-Calibration Data Mapping with Recency-Weighted Exponential Decay
STATE_SEASON_CALIBRATION = {
    "Rajasthan": {
        "Monsoon (Kharif)": (31.0, 420.0, 52.0, 7.8),
        "Winter (Rabi)": (18.0, 150.0, 48.0, 7.8),
        "Summer (Zaid)": (35.0, 100.0, 35.0, 8.0)
    },
    "Punjab": {
        "Monsoon (Kharif)": (29.0, 700.0, 74.0, 6.8),
        "Winter (Rabi)": (16.0, 250.0, 60.0, 6.8),
        "Summer (Zaid)": (33.0, 120.0, 45.0, 7.0)
    },
    "Haryana": {
        "Monsoon (Kharif)": (29.5, 650.0, 72.0, 7.2),
        "Winter (Rabi)": (16.5, 220.0, 58.0, 7.2),
        "Summer (Zaid)": (33.5, 110.0, 42.0, 7.3)
    },
    "Madhya Pradesh": {
        "Monsoon (Kharif)": (26.5, 950.0, 75.0, 7.4),
        "Winter (Rabi)": (19.0, 200.0, 55.0, 7.4),
        "Summer (Zaid)": (33.0, 150.0, 42.0, 7.5)
    },
    "Maharashtra": {
        "Monsoon (Kharif)": (28.0, 850.0, 72.0, 7.5),
        "Winter (Rabi)": (22.0, 180.0, 58.0, 7.5),
        "Summer (Zaid)": (34.0, 100.0, 45.0, 7.6)
    },
    "Uttar Pradesh": {
        "Monsoon (Kharif)": (28.5, 900.0, 76.0, 6.9),
        "Winter (Rabi)": (17.5, 200.0, 62.0, 6.9),
        "Summer (Zaid)": (32.5, 150.0, 48.0, 7.0)
    },
    "Gujarat": {
        "Monsoon (Kharif)": (29.5, 750.0, 70.0, 7.6),
        "Winter (Rabi)": (21.0, 100.0, 52.0, 7.6),
        "Summer (Zaid)": (34.5, 80.0, 40.0, 7.7)
    },
    "Kerala": {
        "Monsoon (Kharif)": (27.0, 2200.0, 85.0, 5.5),
        "Winter (Rabi)": (26.0, 600.0, 78.0, 5.5),
        "Summer (Zaid)": (30.0, 400.0, 75.0, 5.6)
    },
    "West Bengal": {
        "Monsoon (Kharif)": (28.5, 1500.0, 82.0, 6.4),
        "Winter (Rabi)": (20.0, 150.0, 65.0, 6.4),
        "Summer (Zaid)": (31.0, 300.0, 70.0, 6.5)
    },
    "Tamil Nadu": {
        "Monsoon (Kharif)": (30.0, 850.0, 72.0, 6.8),
        "Winter (Rabi)": (25.0, 450.0, 75.0, 6.8),
        "Summer (Zaid)": (33.0, 150.0, 60.0, 7.0)
    },
    "Karnataka": {
        "Monsoon (Kharif)": (26.0, 1100.0, 75.0, 6.5),
        "Winter (Rabi)": (22.0, 200.0, 62.0, 6.5),
        "Summer (Zaid)": (31.0, 120.0, 50.0, 6.7)
    },
    "Bihar": {
        "Monsoon (Kharif)": (28.0, 1050.0, 78.0, 6.8),
        "Winter (Rabi)": (18.0, 120.0, 60.0, 6.8),
        "Summer (Zaid)": (32.0, 150.0, 50.0, 7.0)
    }
}

default_base = STATE_SEASON_CALIBRATION.get(state, {}).get(season, (26.0, 850.0, 68.0, 6.8))
st_calib = RecencyWeightedClimateCalibrator.get_calibrated_climate(
    state=state,
    season=season,
    use_recency_weighting=use_recency,
    default_baseline=default_base
)
p_temp, p_rain, p_hum, p_ph = st_calib

st.sidebar.markdown("---")
st.sidebar.markdown("##### **Climate Sliders**")
temperature = st.sidebar.slider("Temperature (°C)", 10.0, 45.0, p_temp, 0.5)
rainfall = st.sidebar.slider("Seasonal Rainfall (mm)", 100.0, 2500.0, p_rain, 25.0)
humidity = st.sidebar.slider("Relative Humidity (%)", 20.0, 98.0, p_hum, 1.0)

st.sidebar.markdown("##### **Soil Parameters**")
ph = st.sidebar.slider("Soil pH Level", 4.5, 9.0, p_ph, 0.1)

with st.sidebar.expander("🧪 Soil Nutrients (NPK Ratios)", expanded=False):
    nitrogen = st.slider("Nitrogen N (kg/ha)", 0, 140, 40, 5)
    phosphorus = st.slider("Phosphorus P (kg/ha)", 0, 90, 25, 5)
    potassium = st.slider("Potassium K (kg/ha)", 0, 120, 35, 5)

num_crops_display = st.sidebar.slider("Top Crops Display Count", 3, 20, 8)

# Run Prediction Engine with Recency Weighting
results_df = crop_engine.predict_suitability(
    temperature=temperature,
    rainfall=rainfall,
    humidity=humidity,
    ph=ph,
    season=season,
    use_recency_weighting=use_recency
)
top_crop = results_df.iloc[0]


# ==============================================================================
# MINIMAL HEADER SECTION
# ==============================================================================
st.markdown("""
    <div class="minimal-header">
        <h1 class="minimal-title">🌱 AgroSense AI</h1>
        <div class="minimal-subtitle">Precision Micro-Climate & Soil Matching Platform</div>
    </div>
""", unsafe_allow_html=True)

if use_recency:
    st.info("🕒 **Recency-Weighted Seasonal Calibration Active**: Recent 5-year climate metrics (2020–2025) carry 75%+ importance weight in baseline calibration & crop recommendations.")


# ==============================================================================
# MINIMAL METRIC TILES ROW
# ==============================================================================
m1, m2, m3, m4, m5 = st.columns(5)
with m1:
    st.markdown(f'''
        <div class="metric-tile">
            <div class="metric-tile-label">Location</div>
            <div class="metric-tile-value">{city}</div>
            <div class="metric-tile-sub">{district}, {state}</div>
        </div>
    ''', unsafe_allow_html=True)

with m2:
    st.markdown(f'''
        <div class="metric-tile">
            <div class="metric-tile-label">Season</div>
            <div class="metric-tile-value">{season.split()[0]}</div>
            <div class="metric-tile-sub">Cultivation Cycle</div>
        </div>
    ''', unsafe_allow_html=True)

with m3:
    st.markdown(f'''
        <div class="metric-tile">
            <div class="metric-tile-label">Temperature</div>
            <div class="metric-tile-value">{temperature}°C</div>
            <div class="metric-tile-sub">{"Warm" if temperature > 28 else ("Temperate" if temperature >= 18 else "Cool")} Zone</div>
        </div>
    ''', unsafe_allow_html=True)

with m4:
    st.markdown(f'''
        <div class="metric-tile">
            <div class="metric-tile-label">Rainfall</div>
            <div class="metric-tile-value">{rainfall} mm</div>
            <div class="metric-tile-sub">{"High Moisture" if rainfall > 1200 else ("Optimal" if rainfall >= 500 else "Arid/Low")}</div>
        </div>
    ''', unsafe_allow_html=True)

with m5:
    st.markdown(f'''
        <div class="metric-tile">
            <div class="metric-tile-label">Soil pH Profile</div>
            <div class="metric-tile-value">pH {ph}</div>
            <div class="metric-tile-sub">{"Slightly Acidic" if ph < 6.2 else ("Neutral" if ph <= 7.4 else "Alkaline")}</div>
        </div>
    ''', unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)


# ==============================================================================
# MICRO-CLIMATE & SOIL INPUTS OVERVIEW & QUICK TUNING DECK
# ==============================================================================
with st.expander("🎛️ **Micro-Climate & Soil Inputs Deck** (Click to view detailed diagnostics)", expanded=False):
    col_c1, col_c2, col_c3 = st.columns(3)
    
    with col_c1:
        st.markdown("##### 🌡️ **Micro-Climate Diagnostic**")
        st.write(f"• **Temperature**: `{temperature} °C` (Thermal Zone: {'Warm' if temperature > 28 else ('Temperate' if temperature >= 18 else 'Cool')})")
        st.write(f"• **Seasonal Rainfall**: `{rainfall} mm` ({'High Heavy Moisture' if rainfall > 1200 else ('Optimal Moisture' if rainfall >= 500 else 'Arid / Irrigated Required')})")
        st.write(f"• **Relative Humidity**: `{humidity} %` ({'Humid Environment' if humidity > 70 else 'Moderate Air Moisture'})")
        
    with col_c2:
        st.markdown("##### 🧪 **Soil Health & pH Baseline**")
        st.write(f"• **Soil pH Level**: `{ph}` ({'Acidic (Laterite/Red)' if ph < 6.2 else ('Optimal Neutral (Alluvial/Loam)' if ph <= 7.4 else 'Alkaline (Black/Arid Vertisols)')})")
        st.write(f"• **State Baseline Soil Target**: `{STATE_SEASON_CALIBRATION.get(state, {}).get(season, (26, 850, 68, 6.8))[3]}` pH")
        st.write(f"• **Target Crop NPK Guidance**: `N:{top_crop['n_req']} | P:{top_crop['p_req']} | K:{top_crop['k_req']} kg/ha`")

    with col_c3:
        st.markdown("##### ⚡ **Soil Nutrients Applied**")
        st.write(f"• **Nitrogen N**: `{nitrogen} kg/ha`")
        st.write(f"• **Phosphorus P**: `{phosphorus} kg/ha`")
        st.write(f"• **Potassium K**: `{potassium} kg/ha`")


# ==============================================================================
# INTERACTIVE TAB NAVIGATION
# ==============================================================================
tab1, tab2, tab3 = st.tabs([
    "🎯 55-Crop Matcher",
    "🗺️ Soil & Location Explorer",
    "📜 ICAR Knowledge Base"
])

# ------------------------------------------------------------------------------
# TAB 1: CROP RECOMMENDATION MATCHING
# ------------------------------------------------------------------------------
with tab1:
    st.markdown("##### 🌾 **Top 5 Ranked Recommendations**")
    top_5 = results_df.head(5)
    cols = st.columns(5)

    for idx, (_, row) in enumerate(top_5.iterrows()):
        rank_num = idx + 1
        if rank_num == 1:
            badge_title = "✨ TOP MATCH RECOMMENDATION"
            badge_color = "#047857"
            border_style = "2px solid #10B981"
            box_shadow = "0 4px 12px -2px rgba(16, 185, 129, 0.15)"
        else:
            badge_title = f"✨ RANK #{rank_num} RECOMMENDATION"
            badge_color = "#64748B"
            border_style = "1px solid #E2E8F0"
            box_shadow = "0 2px 6px rgba(0, 0, 0, 0.03)"

        with cols[idx]:
            st.markdown(f"""
                <div style="background: #FFFFFF; border: {border_style}; border-radius: 14px; padding: 1.1rem; box-shadow: {box_shadow}; height: 100%;">
                    <div style="font-size: 0.7rem; font-weight: 700; color: {badge_color}; text-transform: uppercase; letter-spacing: 0.05em;">{badge_title}</div>
                    <div style="font-size: 1.4rem; font-weight: 800; color: #0F172A; margin: 0.25rem 0;">🌾 {row['crop']}</div>
                    <div style="font-size: 0.95rem; font-weight: 700; color: #10B981;">Suitability: <strong>{row['suitability_score']}%</strong></div>
                    <hr style="border: 0; border-top: 1px solid #E2E8F0; margin: 0.6rem 0;">
                    <div style="color: #475569; font-size: 0.82rem; line-height: 1.55;">
                        • <strong>Expected Benchmark Yield</strong>: {row['avg_yield_kg_ha']} kg/ha<br>
                        • <strong>Ideal Temperature</strong>: {row['ideal_temp']}°C | <strong>Ideal Rain</strong>: {row['ideal_rain']} mm<br>
                        • <strong>Recommended NPK Ratio</strong>: N: {row['n_req']} | P: {row['p_req']} | K: {row['k_req']} kg/ha
                    </div>
                </div>
            """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown("##### 🤖 **RAG-Powered AI Agronomist Advisory**")
    st.caption("Contextually retrieved from ICAR Package of Practices manuals using ChromaDB Vector Store & Gemini LLM.")

    selected_adv_crop = st.selectbox(
        "Select Crop for ICAR RAG Advisory",
        top_5["crop"].tolist(),
        index=0,
        key="tab1_rag_crop_select",
        help="Select any of the top 5 recommended crops to generate customized ICAR agronomic advisory"
    )

    selected_crop_row = top_5[top_5["crop"] == selected_adv_crop].iloc[0]

    advisory_text = rag_chain.generate_advisory(
        city=city,
        district=district,
        state=state,
        season=season,
        crop_name=selected_crop_row['crop'],
        suitability_score=selected_crop_row['suitability_score'],
        temperature=temperature,
        rainfall=rainfall,
        humidity=humidity,
        ph=ph,
        ideal_temp=float(selected_crop_row.get('ideal_temp', 25.0)),
        ideal_rain=float(selected_crop_row.get('ideal_rain', 600.0)),
        ideal_humidity=float(selected_crop_row.get('ideal_humidity', 65.0)),
        ideal_ph=float(selected_crop_row.get('ideal_ph', 6.5)),
        n_req=float(selected_crop_row.get('n_req', 80.0)),
        p_req=float(selected_crop_row.get('p_req', 40.0)),
        k_req=float(selected_crop_row.get('k_req', 40.0)),
        avg_yield_kg_ha=float(selected_crop_row.get('avg_yield_kg_ha', 2000.0))
    )
    st.markdown(advisory_text)

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown("##### 📊 **Suitability Ranking (Top Crops)**")
    top_n = results_df.head(num_crops_display)

    fig_bar = px.bar(
        top_n,
        x="suitability_score",
        y="crop",
        orientation="h",
        text="suitability_score",
        color="suitability_score",
        color_continuous_scale=["#A7F3D0", "#10B981", "#047857"],
        labels={"suitability_score": "Suitability (%)", "crop": "Crop"}
    )
    fig_bar.update_layout(
        yaxis=dict(autorange="reversed"),
        height=380,
        margin=dict(l=0, r=0, t=10, b=0),
        showlegend=False,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(showgrid=True, gridcolor="#F1F5F9")
    )
    fig_bar.update_traces(texttemplate='%{text}%', textposition='outside')
    st.plotly_chart(fig_bar, use_container_width=True)

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown(f"##### 📋 **Comprehensive 55-Crop Evaluation Matrix** ({len(results_df)} Crops Evaluated)")
    st.dataframe(
        results_df[["crop", "suitability_score", "ideal_temp", "ideal_rain", "ideal_humidity", "avg_yield_kg_ha"]].rename(columns={
            "crop": "Crop Name",
            "suitability_score": "Suitability (%)",
            "ideal_temp": "Ideal Temp (°C)",
            "ideal_rain": "Ideal Rain (mm)",
            "ideal_humidity": "Ideal Humidity (%)",
            "avg_yield_kg_ha": "Expected Yield (kg/ha)"
        }),
        use_container_width=True,
        hide_index=True
    )


# ------------------------------------------------------------------------------
# TAB 2: REGIONAL SOIL, DISTRICT & CITY EXPLORER
# ------------------------------------------------------------------------------
with tab2:
    st.markdown("##### 🗺️ **State, District & City Soil & Location Explorer**")
    st.caption("Interactive multi-level location filtering across 35 States, 508 Districts, and 5,333 Cities.")

    if df_locations is not None and not df_locations.empty:
        col_f1, col_f2, col_f3, col_f4 = st.columns(4)

        with col_f1:
            all_st_options = ["All States"] + sorted(df_locations["state"].unique().tolist())
            default_st_idx = all_st_options.index(state) if state in all_st_options else 0
            tab_state_filter = st.selectbox("Filter State", all_st_options, index=default_st_idx, key="tab2_state")

        df_filtered = df_locations.copy()
        if tab_state_filter != "All States":
            df_filtered = df_filtered[df_filtered["state"] == tab_state_filter]

        with col_f2:
            all_dist_options = ["All Districts"] + sorted(df_filtered["district"].unique().tolist())
            default_dist_idx = all_dist_options.index(district) if district in all_dist_options else 0
            tab_dist_filter = st.selectbox("Filter District", all_dist_options, index=default_dist_idx, key="tab2_district")

        if tab_dist_filter != "All Districts":
            df_filtered = df_filtered[df_filtered["district"] == tab_dist_filter]

        with col_f3:
            all_reg_options = ["All Regions"] + sorted(df_filtered["region"].dropna().unique().tolist())
            tab_reg_filter = st.selectbox("Filter Region", all_reg_options, index=0, key="tab2_region")

        if tab_reg_filter != "All Regions":
            df_filtered = df_filtered[df_filtered["region"] == tab_reg_filter]

        with col_f4:
            search_city_kw = st.text_input("Search City / Keyword", value="", placeholder="e.g. Indore, Pune...", key="tab2_search")

        if search_city_kw:
            df_filtered = df_filtered[
                df_filtered["city"].str.contains(search_city_kw, case=False, na=False) |
                df_filtered["district"].str.contains(search_city_kw, case=False, na=False) |
                df_filtered["state"].str.contains(search_city_kw, case=False, na=False)
            ]

        kpi1, kpi2, kpi3, kpi4 = st.columns(4)
        with kpi1:
            st.metric("States Filtered", df_filtered["state"].nunique())
        with kpi2:
            st.metric("Districts Filtered", df_filtered["district"].nunique())
        with kpi3:
            st.metric("Cities Filtered", len(df_filtered))
        with kpi4:
            st.metric("Regions Covered", df_filtered["region"].nunique())

        st.markdown("<br>", unsafe_allow_html=True)
        col_t1, col_t2 = st.columns([1.2, 0.8])

        with col_t1:
            st.markdown("##### 📋 **Filtered Location Records**")
            display_cols = [c for c in ["city", "district", "state", "region", "lat", "lng"] if c in df_filtered.columns]
            st.dataframe(
                df_filtered[display_cols].rename(columns={
                    "city": "City / Town",
                    "district": "District",
                    "state": "State",
                    "region": "Region",
                    "lat": "Latitude",
                    "lng": "Longitude"
                }),
                use_container_width=True,
                hide_index=True,
                height=350
            )

        with col_t2:
            st.markdown("##### 📊 **Top Districts by City Density**")
            dist_counts = df_filtered["district"].value_counts().head(10).reset_index()
            dist_counts.columns = ["District", "City Count"]
            fig_dist = px.bar(
                dist_counts,
                x="City Count",
                y="District",
                orientation="h",
                color="City Count",
                color_continuous_scale="emrld"
            )
            fig_dist.update_layout(yaxis=dict(autorange="reversed"), height=350, margin=dict(l=0, r=0, t=10, b=0), paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig_dist, use_container_width=True)

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown("##### 🧪 **Indian State Baseline Soil Profiles & pH Explorer**")
    st.caption("Derived from ICAR National Bureau of Soil Survey & Land Use Planning (NBSS & LUP).")

    soil_data = [
        {"State": "Madhya Pradesh", "Primary Soil": "Deep Black Soil (Vertisols)", "Mean pH": 7.4, "Dominant Crops": "Soybean, Wheat, Gram, Maize"},
        {"State": "Maharashtra", "Primary Soil": "Medium to Deep Black Soil", "Mean pH": 7.5, "Dominant Crops": "Cotton, Sugarcane, Jowar, Soybean"},
        {"State": "Punjab", "Primary Soil": "Alluvial Soil (Inceptisols)", "Mean pH": 6.8, "Dominant Crops": "Wheat, Rice, Cotton, Maize"},
        {"State": "Uttar Pradesh", "Primary Soil": "Deep Alluvial Soil", "Mean pH": 6.9, "Dominant Crops": "Sugarcane, Wheat, Rice, Potato"},
        {"State": "Rajasthan", "Primary Soil": "Arid Sandy Soil", "Mean pH": 8.1, "Dominant Crops": "Bajra, Mustard, Guar, Pulses"},
        {"State": "Kerala", "Primary Soil": "Laterite Acidic Soil", "Mean pH": 5.5, "Dominant Crops": "Rubber, Coconut, Black Pepper, Rice"},
        {"State": "West Bengal", "Primary Soil": "Deltaic Alluvial Soil", "Mean pH": 6.4, "Dominant Crops": "Rice, Jute, Potato, Tea"},
        {"State": "Gujarat", "Primary Soil": "Black & Coastal Alluvial", "Mean pH": 7.6, "Dominant Crops": "Cotton, Groundnut, Castor, Cumin"}
    ]
    df_soil = pd.DataFrame(soil_data)

    col_map, col_table = st.columns([1, 1])
    with col_map:
        fig_soil = px.bar(df_soil, x="State", y="Mean pH", color="Mean pH", color_continuous_scale="tealgrn", title="State Baseline Soil pH Distribution")
        fig_soil.update_layout(height=350, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        st.plotly_chart(fig_soil, use_container_width=True)
    with col_table:
        st.dataframe(df_soil, use_container_width=True, hide_index=True)


# ------------------------------------------------------------------------------
# TAB 3: KNOWLEDGE BASE SEARCH
# ------------------------------------------------------------------------------
with tab3:
    st.markdown("##### 📜 **ICAR Agronomy Vector Knowledge Base Search**")
    st.caption("Search vector embeddings stored inside ChromaDB Vector Store.")

    query = st.text_input("Agronomic Query", value="Rice fertilizer NPK", placeholder="e.g. Rice fertilizer schedule, Cotton pink bollworm remedy...")

    if query:
        if rag_chain.vector_store:
            docs = rag_chain.vector_store.similarity_search(query, k=1)
            st.success("Retrieved top matching document chunk from ChromaDB:")
            for i, doc in enumerate(docs):
                with st.expander(f"📄 Top Match Result — Source: {doc.metadata.get('source', 'ICAR Manual')}", expanded=True):
                    st.markdown(doc.page_content)
        else:
            st.warning("ChromaDB vector store initializing...")

# Footer
st.markdown("---")
st.markdown("<div style='text-align: center; color: #94A3B8; font-size: 0.8rem;'>🌱 <strong>AgroSense AI</strong> | Precision Crop & Soil Intelligence</div>", unsafe_allow_html=True)
