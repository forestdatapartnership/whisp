# Annual Crop Risk Decision Tree (soy)

This diagram represents the annual crop risk assessment logic as implemented in `add_risk_acrop_col()` in [src/openforis_whisp/risk.py](../src/openforis_whisp/risk.py).

```mermaid
flowchart TD
    START((Start)) --> A{"Tree cover
    in 2020?"}

    A -->|NO| LOW1[🟢 Low risk]
    A -->|YES| B{"Commodity
    in 2020?"}

    B -->|YES| LOW2[🟢 Low risk]
    B -->|NO| C{"Disturbance
    after 2020?"}

    C -->|YES| HIGH[🔴 High risk]
    C -->|NO| MORE[🟡 More info needed]

    style LOW1 fill:#4CAF50,color:#fff
    style LOW2 fill:#4CAF50,color:#fff
    style HIGH fill:#f44336,color:#fff
    style MORE fill:#FFC107,color:#000
```

## Indicator mapping

| Decision node | Indicator | Code variable |
|---|---|---|
| Tree cover in 2020? | Ind_01 | `ind_1_name` |
| Commodity in 2020? | Ind_02 | `ind_2_name` |
| Disturbance after 2020? | Ind_04 | `ind_4_name` |

## Difference from perennial crop (pcrop) risk

The annual crop decision tree does **not** use Ind_03 (disturbance before 2020). This means prior disturbance is not considered a mitigating factor for soy/annual crop risk, unlike for perennial crops.
