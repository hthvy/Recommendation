from core.data_loader import load_unified_matrices
from core.recommend_config import get_recommend_config, log_recommend_config, normalize_model_to_run
from core.sync_es import sync_data_to_es

from algorithms.search_and_recommend import precompute_all_user_cf_scores
from algorithms.user_based_cf import run_personalized_recommendation


def ai_pipeline_job(model_to_run=None):
    print("==================================================", flush=True)
    print("START AI PIPELINE JOB", flush=True)

    config = get_recommend_config()
    selected_model = normalize_model_to_run(model_to_run or config.get("modelToRun"))

    log_recommend_config(config, "CONFIG LOADED AT PIPELINE START")
    print(f"[MODEL] Selected model to run: {selected_model}", flush=True)
    print("==================================================", flush=True)

    print("[Stage 0] Sync data from backend to Elasticsearch...", flush=True)
    try:
        sync_data_to_es()
    except Exception as e:
        print(f"[WARN] Elasticsearch sync failed, continue with existing ES data: {e}", flush=True)

    user_item_matrix, user_sim = load_unified_matrices()
    if user_item_matrix is None:
        print("[MODEL] Skip pipeline because user-item matrix is empty.", flush=True)
        return

    if selected_model in ("all", "personalized"):
        print("[MODEL] Run Personalized Recommendation branch", flush=True)
        run_personalized_recommendation(user_item_matrix, user_sim)
    else:
        print("[MODEL] Skip Personalized Recommendation branch by admin selection.", flush=True)

    if selected_model in ("all", "search_rerank"):
        print("[MODEL] Run Search Reranking CF precompute branch", flush=True)
        precompute_all_user_cf_scores(user_item_matrix, user_sim)
    else:
        print("[MODEL] Skip Search Reranking branch by admin selection.", flush=True)

    print("[MODEL] AI pipeline job finished.", flush=True)
    print("==================================================", flush=True)


if __name__ == "__main__":
    ai_pipeline_job()
