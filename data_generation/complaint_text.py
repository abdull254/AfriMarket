"""
Builds varied complaint text from small sentence pieces.

Each complaint = greeting (optional) + main sentence + detail (optional) + ask.
Main sentences fill in real details from the order (product, courier, amount,
days waited, seller), so two complaints in the same category rarely read the same.

Most texts in the fraud categories still contain the words that the keyword
rules in complaint_analysis.py look for ("unauthorised", "never received", ...),
but some are written without them on purpose, like real customers do, so the
rules no longer match 100% of the time.
"""

GREETINGS = ["", "", "", "Hello, ", "Hi team, ", "Good morning, ", "Habari, ", "Please help, ",
             "Dear AfriMarket, ", "Hello AfriMarket support, "]

MAIN = {
    "Missing Package": [
        "I never received my {product} ordered {days} days ago.",
        "my {product} still has not arrived and it has been {days} days.",
        "the app says delivered but I never received the {product}.",
        "tracking shows delivery by {courier} but nothing was delivered to me.",
        "my order has not arrived yet, {days} days after I paid {amount}.",
        "{courier} marked my parcel as delivered but no one came to my address.",
        "I have been waiting {days} days for the {product} and there is no update.",
    ],
    "Refund Issue": [
        "my refund of {amount} has not been processed.",
        "I was promised a refund for the {product} but the money has not been returned to my {method}.",
        "it has been {days} days and I still have not received my refund of {amount}.",
        "I returned the {product} but no refund has reached my account.",
        "the refund status has said pending for {days} days.",
        "you approved my refund but {amount} is still missing from my {method}.",
    ],
    "Wrong Item": [
        "the item in my package does not match the product description.",
        "I ordered a {product} but received something different.",
        "I got the wrong size and colour of {product}.",
        "the parcel from {seller} had another customer's order inside.",
        "the {product} I received is not the model shown on the listing.",
    ],
    "Damaged Product": [
        "my {product} arrived damaged and I would like a replacement.",
        "the {product} was broken when {courier} delivered it.",
        "the box was crushed and the {product} does not work.",
        "my {product} stopped working after one day.",
        "the {product} arrived with missing parts and a broken cover.",
    ],
    "Suspicious Seller": [
        "the seller {seller} looks suspicious, they asked me to pay outside the app.",
        "I think {seller} is a scam, they took {amount} and stopped responding.",
        "{seller} is a fake seller, the listing photos are copied from another shop.",
        "the seller sent me a link to pay by {method} directly, this looks like fraud.",
        "{seller} keeps changing the price after payment and will not answer.",
        "the seller's reviews look fake and the item never matches, please check {seller}.",
    ],
    "Unauthorized Charge": [
        "I did not authorise a charge of {amount} on my {method}.",
        "there is a payment of {amount} that I never placed.",
        "I do not recognize this transaction for a {product}.",
        "an unauthorized payment of {amount} was taken from my {method}.",
        "someone used my account to buy a {product}, it was not me.",
        "my {method} was charged {amount} while my phone was with me the whole day.",
        "I think my card was stolen, there is a {amount} order I know nothing about.",
    ],
    "Late Delivery": [
        "my {product} was supposed to arrive {days} days ago.",
        "{courier} keeps moving the delivery date for my {product}.",
        "the delivery is very late, I was promised it within {promised} days.",
    ],
    "Chargeback Dispute": [
        "my bank reversed a payment of {amount} and I want to know why.",
        "I raised a dispute with my bank for {amount} and need an update.",
    ],
}

DETAIL = ["", "", "", "I have called twice already.", "This is my second complaint about this.",
          "Order reference is {tid}.", "I paid with {method}.", "I need this resolved this week.",
          "This is not the first time this has happened.", "I have photos if you need them."]

ASK = {
    "Missing Package": ["Please tell me where my parcel is.", "Please send it or refund me.", "Kindly follow up with the courier."],
    "Refund Issue": ["Please process it.", "When will I get my money back?", "Please check and reply."],
    "Wrong Item": ["Please send the correct item.", "How do I return it?", "I want an exchange."],
    "Damaged Product": ["Please replace it.", "I want my money back.", "How do I return it?"],
    "Suspicious Seller": ["Please investigate this seller.", "Please block this seller.", "I want my money back."],
    "Unauthorized Charge": ["Please reverse it.", "Please block my account until this is checked.", "Refund me immediately."],
    "Late Delivery": ["Please give me a real date.", "Please hurry.", "Otherwise cancel and refund."],
    "Chargeback Dispute": ["Please reply.", "Please check with the bank."],
}


def build(category, rng, **slots):
    """Return one complaint sentence for this category, filled with order details."""
    main = MAIN[category][rng.integers(len(MAIN[category]))]
    parts = [GREETINGS[rng.integers(len(GREETINGS))] + main,
             DETAIL[rng.integers(len(DETAIL))],
             ASK[category][rng.integers(len(ASK[category]))]]
    text = " ".join(p for p in parts if p).format(**slots)
    text = text[0].upper() + text[1:]
    if rng.random() < 0.06:          # some customers type in lower case
        text = text.lower()
    return text
