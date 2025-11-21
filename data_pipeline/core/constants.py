NEWS_CATEGORIES: dict[str, str] = {
    "core_financial": "stocks OR earnings OR investment OR market trend OR merger OR acquisition OR financial report",
    "macro_politics": "politics OR election OR central bank OR Fed OR policy OR legislature OR government spending OR opinion column",
    "behavioral_mood": "World Cup OR NBA Finals OR Olympics OR celebrity OR movie review OR music industry OR award ceremony OR athlete",
    "innovation_tech": "AI OR tech innovation OR medical breakthrough OR space exploration OR corporate scandal OR data breach OR investigation",
    "general_sentiment": "lifestyle OR travel OR health trend OR social issue OR climate change OR crime OR court verdict"
}

SUBREDDITS = [
    {"name": "TrueReddit", "flairs": ["Business + Economics", "Energy + Environment", "Technology"]},
    {"name": "StockMarket", "flairs": ["News", "Analysis", "Opinion"]},
    {"name": "technology", "flairs": ["Artificial Intelligence", "Business", "Software", "Security"]},
    {"name": "news", "flairs": []},
    {"name": "FinanceNews", "flairs": []},
]

STOCK_TICKERS = ["AAPL", "AMD", "NVDA", "TSLA", "SNOW", "MSFT", "ORCL", "META", "SAP", "CSCO", "SHEL", "MCD", "UBER", "QCOM", "INTC"]

DEFAULT_ARTICLE_DATA = {
    "article_headline": None,
    "article_author": None,
    "article_publisher": None,
    "article_content": None,
    "article_published_at": None,
    "article_category": None,
}

DATA_FETCH_LIMIT_PER_FLOW = 100
