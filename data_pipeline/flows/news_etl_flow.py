from prefect import flow
from tasks.extraction import extract_news_data
from tasks.transformation import transform_news_data
from tasks.loading import dispatch_load_news_data


@flow(name="NewsAPI ETL Pipeline", log_prints=True)
def news_etl_flow():
    """
    Dedicated ETL pipeline for NewsAPI.
    """

    categories: dict[str, str] = {
        # CORE FINANCIAL NEWS (Business News, Hard News - Economics)
        # High-signal for direct financial impact.
        "core_financial": "stocks OR earnings OR investment OR market trend OR merger OR acquisition OR financial report",
        # MACRO / POLITICAL RISK (Hard News - Politics, Editorial/Opinion)
        # Captures broad systemic risk and policy uncertainty.
        "macro_politics": "politics OR election OR central bank OR Fed OR policy OR legislature OR government spending OR opinion column",
        # BEHAVIORAL MOOD / SPORTS & ENTERTAINMENT (Sports, Entertainment)
        # Measures general investor optimism/pessimism from non-financial events.
        "behavioral_mood": "World Cup OR NBA Finals OR Olympics OR celebrity OR movie review OR music industry OR award ceremony OR athlete",
        # INNOVATION & DISRUPTORS (Science/Technology, Investigative)
        # Tracks long-term technological drivers and related controversies.
        "innovation_tech": "AI OR tech innovation OR medical breakthrough OR space exploration OR corporate scandal OR data breach OR investigation",
        # GENERAL SENTIMENT (Soft News, Feature News, Crime/Legal)
        # Captures consumer interest, social trends, and general societal distress.
        "general_sentiment": "lifestyle OR travel OR health trend OR social issue OR climate change OR crime OR court verdict"
    }
    
    # 1. E-xtraction
    print(f"*** Running News ETL for query: {categories["core_financial"]} ***")
    raw_data = extract_news_data(query=categories["core_financial"])

    # 2. T-ransformation 
    transformed_data = transform_news_data(raw_data)
    
    # 3. L-oading 
    # todo: ♻️ need to automate such that we pass on the category 
    # that is used to extract the data from the api
    dispatch_load_news_data(transformed_data, "core_financial")
    
    return len(raw_data) # Return count for aggregation
