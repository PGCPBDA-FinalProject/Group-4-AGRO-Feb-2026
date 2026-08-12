#!/usr/bin/env python3
"""
==============================================================================
PHASE 3: RAG ORCHESTRATION & AGRO ADVISORY GENERATOR CHAIN
==============================================================================
Description: Combines Quantitative Engine crop matches (Phase 1) with retrieved
             agronomy context from ChromaDB (Phase 2), querying Google Gemini / LLM
             to produce structured, actionable farming advisories.
==============================================================================
"""

import os
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("RAGChain")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CHROMA_DB_DIR = os.path.join(BASE_DIR, "chroma_db")


class AgroRAGAdvisoryChain:
    """RAG Advisory Chain integrating ChromaDB Vector Retrieval with LLM Generation."""

    def __init__(self, chroma_dir: str = CHROMA_DB_DIR):
        self.chroma_dir = chroma_dir
        self.vector_store = None
        self.pkl_chunks = None
        self._init_vector_store()
        self._init_llm()

    def _init_vector_store(self):
        """Loads persistent ChromaDB vector store or falls back to local PKL knowledge cache."""
        cache_pkl_path = os.path.join(BASE_DIR, "cache", "knowledge_chunks_cache.pkl")
        if os.path.exists(cache_pkl_path):
            try:
                import pickle
                with open(cache_pkl_path, "rb") as f:
                    self.pkl_chunks = pickle.load(f)
                logger.info(f"Loaded {len(self.pkl_chunks)} document chunks from local PKL cache ({cache_pkl_path})")
            except Exception as e:
                logger.warning(f"Notice loading local PKL knowledge cache: {e}")

        if not os.path.exists(self.chroma_dir):
            logger.warning(f"ChromaDB path {self.chroma_dir} not found. Operating with local PKL cache.")
            return

        try:
            from langchain_community.vectorstores import Chroma
            from langchain_community.embeddings import HuggingFaceEmbeddings

            embeddings = HuggingFaceEmbeddings(model_name="all-MiniLM-L6-v2")
            self.vector_store = Chroma(persist_directory=self.chroma_dir, embedding_function=embeddings)
            logger.info(f"Loaded ChromaDB vector store from {self.chroma_dir}")
        except Exception as e:
            logger.warning(f"ChromaDB vector store load notice ({e}). Operating in Local PKL / Template mode.")

    def _init_llm(self):
        """Initializes LLM engine (Google Gemini API via langchain_google_genai or expert fallback)."""
        self.api_key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
        self.llm = None

        if self.api_key:
            try:
                from langchain_google_genai import ChatGoogleGenerativeAI
                self.llm = ChatGoogleGenerativeAI(
                    model="gemini-1.5-flash",
                    google_api_key=self.api_key,
                    temperature=0.2
                )
                logger.info("Google Gemini 1.5 Flash LLM engine initialized successfully.")
            except Exception as e:
                logger.warning(f"Could not initialize ChatGoogleGenerativeAI ({e}). Operating in Expert Template Mode.")
        else:
            logger.info("No GEMINI_API_KEY detected in environment. Operating in Expert Template Mode.")

    def generate_advisory(
        self,
        city: str,
        season: str,
        crop_name: str,
        suitability_score: float,
        temperature: float,
        rainfall: float,
        humidity: float,
        ph: float = 6.5,
        district: str = None,
        state: str = None,
        ideal_temp: float = 25.0,
        ideal_rain: float = 600.0,
        ideal_humidity: float = 65.0,
        ideal_ph: float = 6.5,
        n_req: float = 80.0,
        p_req: float = 40.0,
        k_req: float = 40.0,
        avg_yield_kg_ha: float = 2000.0
    ) -> str:
        """
        Retrieves agronomic context for the crop and generates a data-driven natural language farming advisory.
        """
        # Build comprehensive location string
        loc_parts = []
        if city:
            loc_parts.append(f"City: {city}")
        if district and district.lower() != (city.lower() if city else ""):
            loc_parts.append(f"District: {district}")
        if state:
            loc_parts.append(f"State: {state}")
        location_str = ", ".join(loc_parts) if loc_parts else city

        # 1. Retrieve Knowledge Base Context from ChromaDB
        retrieved_context = ""
        doc_sources = []
        if self.vector_store:
            try:
                # Build dynamic semantic tags based on active micro-climate & soil parameters
                temp_tag = "high temperature heat stress tolerance" if temperature > 30 else ("cool temperature frost sensitivity" if temperature < 18 else "optimal temperature growth")
                rain_tag = "drought water stress low rainfall" if rainfall < 400 else ("heavy rainfall drainage excess moisture" if rainfall > 1200 else "irrigation rainfall schedule")
                ph_tag = "acidic soil amendment" if ph < 6.2 else ("alkaline saline soil management" if ph > 7.5 else "neutral soil NPK fertility")

                query = f"{crop_name} cultivation {temp_tag} {rain_tag} {ph_tag} fertilizer pest disease"
                docs = self.vector_store.similarity_search(query, k=1)
                if docs:
                    cleaned_chunks = []
                    seen_texts = set()
                    for d in docs:
                        src = d.metadata.get('source', 'ICAR Guide')
                        raw_text = d.page_content.strip()
                        if raw_text in seen_texts:
                            continue
                        seen_texts.add(raw_text)

                        # Convert Markdown headers cleanly to bold text (eliminating '#' pollution)
                        lines = []
                        for line in raw_text.splitlines():
                            if line.startswith("#"):
                                header_text = line.lstrip("#").strip()
                                lines.append(f"**{header_text}**")
                            else:
                                lines.append(line)
                        cleaned_content = "\n".join(lines)
                        cleaned_chunks.append(f"**Source ({src})**:\n{cleaned_content}")

                    retrieved_context = "\n\n".join(cleaned_chunks)
                    logger.info(f"Retrieved {len(cleaned_chunks)} unique context chunks for crop '{crop_name}' with query '{query}' from ChromaDB.")
            except Exception as search_err:
                logger.warning(f"ChromaDB search notice ({search_err}).")

        # 1b. Fallback: Retrieve from Local PKL Knowledge Cache if ChromaDB is unavailable
        if not retrieved_context and self.pkl_chunks:
            try:
                crop_lower = crop_name.lower()
                matched_chunks = []
                for chunk in self.pkl_chunks:
                    text = chunk.get("page_content", "")
                    if crop_lower in text.lower():
                        src = chunk.get("metadata", {}).get("source", "PKL Cache")
                        matched_chunks.append(f"**Source ({src}) [Local PKL Cache]**:\n{text}")
                        if len(matched_chunks) >= 2:
                            break
                if matched_chunks:
                    retrieved_context = "\n\n".join(matched_chunks)
                    logger.info(f"Retrieved {len(matched_chunks)} context chunks for crop '{crop_name}' from Local PKL Cache.")
            except Exception as pkl_err:
                logger.warning(f"Local PKL chunk search notice: {pkl_err}")

        # 2. Build LLM Prompt
        prompt = f"""
You are a Lead Agronomist at the Indian Council of Agricultural Research (ICAR). Provide an actionable, data-driven farming advisory.

LOCATION & WEATHER CONTEXT:
- Location: {location_str}
- Season: {season}
- User Input Conditions: Temp = {temperature}°C, Rain = {rainfall}mm, Relative Humidity = {humidity}%, Soil pH = {ph}

DATASET CROP AGRONOMIC BASELINES:
- Target Crop: {crop_name} (Suitability Score: {suitability_score}%)
- Dataset Ideal Baseline: Temp = {ideal_temp}°C, Rain = {ideal_rain}mm, Humidity = {ideal_humidity}%, pH = {ideal_ph}
- Dataset NPK Requirement: N: {n_req} kg/ha | P: {p_req} kg/ha | K: {k_req} kg/ha
- Expected Benchmark Yield: {avg_yield_kg_ha} kg/ha

RETRIEVED ICAR AGRONOMY GUIDELINES:
{retrieved_context if retrieved_context else "Standard agronomic practices apply."}

Please format your response into the following 4 structured sections:
1. 🌱 Sowing Window & Climate Assessment
2. 🧪 Recommended NPK Fertilizer & Soil Nutrition Schedule
3. 🐛 Pest & Disease Risk Alerts (Prevention & Remedies)
4. 💧 Irrigation & Water Management Strategy
"""

        # 3. LLM Inference or Fallback Generator
        if self.llm:
            try:
                response = self.llm.invoke(prompt)
                return response.content
            except Exception as err:
                logger.warning(f"LLM API call failed ({err}). Using fallback generator.")

        # Fallback Offline Expert Advisory Generator
        return self._generate_fallback_advisory(
            crop_name, location_str, season, suitability_score, temperature, rainfall, humidity, ph,
            ideal_temp, ideal_rain, ideal_humidity, ideal_ph, n_req, p_req, k_req, avg_yield_kg_ha, retrieved_context
        )

    def _generate_fallback_advisory(
        self, crop, location_str, season, score, temp, rain, hum, ph,
        ideal_temp, ideal_rain, ideal_hum, ideal_ph, n_req, p_req, k_req, avg_yield, context
    ):
        """Generates a data-driven agronomic advisory computing direct comparisons against dataset profile baselines."""
        temp_diff = temp - ideal_temp
        rain_diff = rain - ideal_rain
        hum_diff = hum - ideal_hum
        ph_diff = ph - ideal_ph

        # 1. Thermal Diagnostic & Sowing Strategy (Data-Driven Temperature Comparison)
        if abs(temp_diff) <= 2.5:
            temp_advice = f"Observed temperature of **{temp}°C** matches **{crop}'s dataset baseline ideal of {ideal_temp}°C** (Deviation: **{temp_diff:+.1f}°C**). Thermal units are optimal for germination."
            sowing_strategy = f"Standard sowing window for **{season}** season. Prepare fine seedbed tilth and sow at standard 4–5 cm depth with 30 cm row spacing."
        elif temp_diff > 2.5:
            temp_advice = f"Observed temperature of **{temp}°C** is **{temp_diff:+.1f}°C warmer** than {crop}'s dataset ideal threshold of **{ideal_temp}°C**. Elevated evapotranspiration requires soil heat mitigation."
            sowing_strategy = f"Sow heat-tolerant certified seed strains. Apply surface straw mulching (5 t/ha) immediately after sowing to regulate seedbed temperature."
        else:
            temp_advice = f"Observed temperature of **{temp}°C** is **{temp_diff:+.1f}°C cooler** than {crop}'s dataset ideal threshold of **{ideal_temp}°C**."
            sowing_strategy = f"Ensure seed treatment with phosphatic bio-fertilizers. Delay irrigation until soil temperature reaches **{ideal_temp}°C** to prevent cold seed rot."

        # 2. Soil pH Optimization & Fertilizer Strategy (Data-Driven NPK Profile)
        if abs(ph_diff) <= 0.5:
            soil_advice = f"Soil pH of **{ph}** is well-balanced with **{crop}'s dataset ideal pH of {ideal_ph}**."
            nutrient_strategy = f"Apply baseline dataset NPK requirement: **N: {n_req:.0f} kg/ha | P: {p_req:.0f} kg/ha | K: {k_req:.0f} kg/ha** with 5 t/ha FYM compost to achieve expected benchmark yield of **{avg_yield:,.0f} kg/ha**."
        elif ph < 6.0:
            soil_advice = f"Soil pH of **{ph}** is acidic relative to **{crop}'s ideal baseline of {ideal_ph}**."
            nutrient_strategy = f"Incorporate Agricultural Lime (CaCO3 @ 2.0 t/ha). Apply dataset P requirement (**{p_req:.0f} kg P2O5/ha**) as Single Super Phosphate (SSP) supplying essential Calcium & Sulphur."
        else:
            soil_advice = f"Soil pH of **{ph}** is alkaline relative to **{crop}'s ideal baseline of {ideal_ph}**."
            nutrient_strategy = f"Apply Gypsum at 1.5 t/ha + Zinc Sulphate (25 kg/ha) with dataset NPK profile (**N: {n_req:.0f} | P: {p_req:.0f} | K: {k_req:.0f} kg/ha**)."

        # 3. Humidity Impact & Disease / Pest Alerts (Data-Driven Humidity Comparison)
        if hum_diff > 10.0:
            hum_advice = f"Observed relative humidity of **{hum}%** is **{hum_diff:+.1f}% higher** than {crop}'s dataset baseline ideal of **{ideal_hum}%**."
            pest_advice = f"HIGH DISEASE RISK: Excessive air moisture increases threat of Downy Mildew, Rust, and Leaf Blight. Spray Hexaconazole 5% EC (2ml/L) or Mancozeb 75 WP (2.5g/L)."
        elif hum_diff < -15.0:
            hum_advice = f"Observed relative humidity of **{hum}%** is **{hum_diff:+.1f}% drier** than {crop}'s dataset baseline ideal of **{ideal_hum}%**."
            pest_advice = f"PEST ALERT: Dry atmosphere accelerates Spider Mite & Pod Borer / Armyworm outbreaks. Install Pheromone traps (5/acre) and spray Emamectin Benzoate 5% SG (0.4g/L)."
        else:
            hum_advice = f"Observed relative humidity of **{hum}%** aligns with **{crop}'s dataset baseline ideal of {ideal_hum}%**."
            pest_advice = f"LOW DISEASE PRESSURE: Monitor for sucking pests (Aphids, Jassids, Thrips). Install yellow sticky traps (10/acre) and spray Neem oil 1500 ppm (5ml/L) at pest onset."

        # 4. Rainfall Adaptation & Irrigation Strategy (Data-Driven Rainfall Comparison)
        if rain_diff < -200.0:
            rain_advice = f"Observed seasonal rainfall of **{rain} mm** shows a deficit of **{abs(rain_diff):.0f} mm** against {crop}'s dataset baseline requirement of **{ideal_rain:.0f} mm**."
            irrigation_strategy = f"Drip / Sprinkler micro-irrigation mandatory to bridge the **{abs(rain_diff):.0f} mm deficit**. Mulch inter-rows with crop residue to conserve moisture."
        elif rain_diff > 300.0:
            rain_advice = f"Observed seasonal rainfall of **{rain} mm** shows a surplus of **{rain_diff:+.0f} mm** over {crop}'s dataset baseline requirement of **{ideal_rain:.0f} mm**."
            irrigation_strategy = f"Construct broad-bed furrows (BBF) and field boundary drainage ditches (30 cm depth) to evacuate standing water and prevent root rot."
        else:
            rain_advice = f"Observed seasonal rainfall of **{rain} mm** matches **{crop}'s dataset baseline requirement of {ideal_rain:.0f} mm** (Deviation: **{rain_diff:+.0f} mm**)."
            irrigation_strategy = f"Rainfed baseline optimal. Provide 2 supplemental irrigations at flowering and pod/grain development if dry spell exceeds 15 days."

        return f"""### 🌾 Data-Driven ICAR Agronomic Advisory for {crop} ({season} Season)

**Target Location**: {location_str} | **Suitability Index**: **{score}%**  
**Observed vs Dataset Baselines**: Temp: **{temp}°C** (Ideal: {ideal_temp}°C) | Rain: **{rain}mm** (Ideal: {ideal_rain}mm) | Humidity: **{hum}%** (Ideal: {ideal_hum}%) | Soil pH: **{ph}** (Ideal: {ideal_ph})

---

#### 1. 🌱 Sowing Window & Climate Assessment
- **Thermal Diagnostic**: {temp_advice}
- **Sowing & Canopy Strategy**: {sowing_strategy}

#### 2. 🧪 Recommended NPK Fertilizer & Soil Nutrition Schedule
- **Soil pH Optimization**: {soil_advice}
- **Nutrient & Fertilizer Schedule**: {nutrient_strategy}

#### 3. 🐛 Pest & Disease Risk Alerts (Prevention & Remedies)
- **Humidity Diagnostic**: {hum_advice}
- **Pest & Disease Action Plan**: {pest_advice}

#### 4. 💧 Irrigation & Water Management Strategy
- **Rainfall Adaptation**: {rain_advice}
- **Irrigation Action Plan**: {irrigation_strategy}
"""


if __name__ == "__main__":
    chain = AgroRAGAdvisoryChain()
    advisory = chain.generate_advisory(
        city="Jaipur",
        district="Jaipur",
        state="Rajasthan",
        season="Winter (Rabi)",
        crop_name="Chickpea",
        suitability_score=76.86,
        temperature=19.0,
        rainfall=550.0,
        humidity=58.0,
        ph=6.5
    )
    print("\n" + advisory)
