def get_odds_from_price(X: int):
    if X > 0:
        return 1 + (X / 100)
    elif X < 0:
        return 1 - (100 / X)
    else:
        return None