#!/usr/bin/env python3
"""
==============================================================================
PHASE 2: RAG KNOWLEDGE BASE INGESTION & CHROMA VECTOR DB SETUP
==============================================================================
Description: Loads agronomy manuals/markdown files from rag_app/knowledge_base/,
             chunks text, generates vector embeddings using sentence-transformers,
             and persists a local ChromaDB vector store in rag_app/chroma_db/.
==============================================================================
"""

import os
import glob
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("RAGIngestion")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
KNOWLEDGE_BASE_DIR = os.path.join(BASE_DIR, "knowledge_base")
CHROMA_DB_DIR = os.path.join(BASE_DIR, "chroma_db")


def load_documents():
    """Reads all Markdown and Text files from knowledge_base directory."""
    documents = []
    logger.info(f"Scanning knowledge base files in {KNOWLEDGE_BASE_DIR}...")

    try:
        from langchain_core.documents import Document
    except ImportError:
        logger.error("langchain_core package not found. Run pip install langchain-core")
        return []

    file_paths = glob.glob(os.path.join(KNOWLEDGE_BASE_DIR, "*.md")) + glob.glob(os.path.join(KNOWLEDGE_BASE_DIR, "*.txt"))

    for path in file_paths:
        try:
            with open(path, "r", encoding="utf-8") as f:
                text = f.read()
                filename = os.path.basename(path)
                doc = Document(page_content=text, metadata={"source": filename})
                documents.append(doc)
                logger.info(f"Loaded document: {filename} ({len(text)} chars)")
        except Exception as e:
            logger.error(f"Error loading {path}: {e}")

    return documents


def chunk_documents(documents):
    """Splits documents into overlapping chunks for semantic retrieval."""
    if not documents:
        return []

    try:
        from langchain_text_splitters import RecursiveCharacterTextSplitter
    except ImportError:
        logger.error("langchain_text_splitters package not found.")
        return []

    logger.info("Splitting documents into chunks...")
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=600,
        chunk_overlap=80,
        separators=["\n## ", "\n### ", "\n\n", "\n", " "]
    )
    chunks = text_splitter.split_documents(documents)
    logger.info(f"Generated {len(chunks)} text chunks from {len(documents)} source documents.")
    return chunks


def build_vector_store():
    """Generates embeddings and builds persistent ChromaDB vector store."""
    docs = load_documents()
    if not docs:
        logger.error("No knowledge base documents found or missing required dependencies!")
        return None

    chunks = chunk_documents(docs)
    if not chunks:
        logger.error("Chunking produced 0 text chunks!")
        return None

    # 0. Always save local PKL cache of document chunks and raw text for fast offline fallback
    try:
        import pickle
        cache_dir = os.path.join(BASE_DIR, "cache")
        os.makedirs(cache_dir, exist_ok=True)
        cache_pkl_path = os.path.join(cache_dir, "knowledge_chunks_cache.pkl")
        
        chunk_data = [
            {"page_content": chunk.page_content, "metadata": chunk.metadata}
            for chunk in chunks
        ]
        with open(cache_pkl_path, "wb") as f:
            pickle.dump(chunk_data, f)
        logger.info(f"Persisted {len(chunk_data)} text chunks to local PKL cache -> {cache_pkl_path}")
    except Exception as e:
        logger.warning(f"Failed to persist local PKL chunk cache: {e}")

    try:
        from langchain_community.vectorstores import Chroma
        from langchain_community.embeddings import HuggingFaceEmbeddings
    except ImportError as err:
        logger.error(f"Required RAG vector store imports failed ({err}). Local PKL cache is available for offline mode!")
        return None

    logger.info("Initializing SentenceTransformers Embedding Model ('all-MiniLM-L6-v2')...")
    embeddings = HuggingFaceEmbeddings(model_name="all-MiniLM-L6-v2")

    # Clean existing ChromaDB directory to prevent duplicate document accumulation
    if os.path.exists(CHROMA_DB_DIR):
        import shutil
        logger.info(f"Clearing old ChromaDB directory at {CHROMA_DB_DIR} to eliminate duplicates...")
        shutil.rmtree(CHROMA_DB_DIR, ignore_errors=True)

    logger.info(f"Creating persistent ChromaDB vector store at {CHROMA_DB_DIR}...")
    vector_store = Chroma.from_documents(
        documents=chunks,
        embedding=embeddings,
        persist_directory=CHROMA_DB_DIR
    )

    logger.info("=" * 70)
    logger.info("SUCCESS: RAG Knowledge Base Ingestion & Vector DB Built Cleanly!")
    logger.info("=" * 70)
    return vector_store


if __name__ == "__main__":
    build_vector_store()
