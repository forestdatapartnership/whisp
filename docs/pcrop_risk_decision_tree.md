# Perennial Crop Risk Decision Tree (coffee, cocoa, rubber, palm oil)

This diagram represents the perennial crop risk assessment logic as implemented in `add_risk_pcrop_col()` in [src/openforis_whisp/risk.py](../src/openforis_whisp/risk.py).

```mermaid
flowchart TD
    START((Start)) --> A{"Tree cover
    in 2020?"}

    A -->|NO| LOW1[🟢 Low risk]
    A -->|YES| B{"Commodity
    in 2020?"}

    B -->|YES| LOW2[🟢 Low risk]
    B -->|NO| C{"Disturbance
    before 2020?"}

    C -->|YES| LOW3[🟢 Low risk]
    C -->|NO| D{"Disturbance
    after 2020?"}

    D -->|YES| HIGH[🔴 High risk]
    D -->|NO| MORE[🟡 More info needed]

    style LOW1 fill:#4CAF50,color:#fff
    style LOW2 fill:#4CAF50,color:#fff
    style LOW3 fill:#4CAF50,color:#fff
    style HIGH fill:#f44336,color:#fff
    style MORE fill:#FFC107,color:#000
```

## Indicator mapping

| Decision node | Indicator | Code variable |
|---|---|---|
| Tree cover in 2020? | Ind_01 | `ind_1_name` |
| Commodity in 2020? | Ind_02 | `ind_2_name` |
| Disturbance before 2020? | Ind_03 | `ind_3_name` |
| Disturbance after 2020? | Ind_04 | `ind_4_name` |

## Note

The code evaluates the first three conditions (`ind_1 == "no"`, `ind_2 == "yes"`, `ind_3 == "yes"`) as a single OR check — any one of them being true results in Low risk. The diagram shows them sequentially for clarity, but the order between them does not matter in practice.
