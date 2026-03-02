# Timber Risk Decision Tree

This diagram represents the timber risk assessment logic as implemented in `add_risk_timber_col()` in [src/openforis_whisp/risk.py](../src/openforis_whisp/risk.py).

```mermaid
flowchart TD
    START((Start)) --> A{"Agriculture/Commodity
    in 2020?"}

    A -->|YES| LOW1[🟢 Low risk]
    A -->|NO| B{"Planted/plantation
    in 2020?"}

    B -->|YES| C{"Agriculture
    after 2020?"}
    C -->|NO| LOW2[🟢 Low risk]
    C -->|YES| HIGH1[🔴 High risk]

    B -->|NO| D{"Natural forest in 2020?
    Primary OR
    Naturally regenerating"}

    D -->|YES| E{"Agriculture
    after 2020?"}
    E -->|YES| HIGH2[🔴 High risk]
    E -->|NO| F{"Planted/plantation
    after 2020?"}

    F -->|YES| HIGH3[🔴 High risk]
    F -->|NO| G{"Regrowth or
    Logging concession?"}

    G -->|YES| LOW3[🟢 Low risk]
    G -->|NO| MORE[🟡 More info needed]

    D -->|NO| LOW4[🟢 Low risk]

    style LOW1 fill:#4CAF50,color:#fff
    style LOW2 fill:#4CAF50,color:#fff
    style LOW3 fill:#4CAF50,color:#fff
    style LOW4 fill:#4CAF50,color:#fff
    style HIGH1 fill:#f44336,color:#fff
    style HIGH2 fill:#f44336,color:#fff
    style HIGH3 fill:#f44336,color:#fff
    style MORE fill:#FFC107,color:#000
```

## Indicator mapping

| Decision node | Indicator | Code variable |
|---|---|---|
| Agriculture/Commodity in 2020? | Ind_02 | `ind_2_name` |
| Planted/plantation in 2020? | Ind_07 | `ind_7_name` |
| Natural forest in 2020? (Primary) | Ind_05 | `ind_5_name` |
| Natural forest in 2020? (Naturally regenerating) | Ind_06 | `ind_6_name` |
| Agriculture after 2020? | Ind_10 | `ind_10_name` |
| Planted/plantation after 2020? | Ind_08 | `ind_8_name` |
| Regrowth (treecover after 2020)? | Ind_09 | `ind_9_name` |
| Logging concession? | Ind_11 | `ind_11_name` |
