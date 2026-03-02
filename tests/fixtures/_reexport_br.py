import ee

ee.Initialize(project="ee-whisp")

br_ids = [
    "NK6IUG3AFTLPJS6RT6PKKV6F",
    "INNA22RBXOUZXLU3PWTMFYDW",
    "EARMB35HETOEDP2R62X3ZWPZ",
    "RALBMXFPEPMAIZ2TRN4EM4V6",
    "VHOAWLXETR4TIPHWGMZOQXYZ",
    "J4U3FK2BHFQ2XYC77GTOIVKB",
    "HEDYH2H6C3GJQA5M5NY45QMO",
    "NIGOOTG44UKDGWEB4VWQS2BO",
    "LKRQD6FGSR6AHNMPHPZ5TGII",
    "TLGXJMK47GC22LFEQSKOOB67",
    "JEHWYTJSOPHRZQDQ5FIUPGYJ",
    "TSYW76JQOQY7D4LLNJGV7SDE",
    "EVVCE3BNRL7DAQ4DXJH57YQX",
    "3PN3R6YZZZ223YA2F2MNUUIV",
    "TXAJJO5EGT3MSLCWQ2OWLIVU",
    "TKTFLOAWGHCOHSLJDPVTMR2S",
    "L2GPHI3AYPZR3CLQRYF6UWNG",
    "7BT4WA5EVTJ2MG2SGZWYHGCJ",
    "S3CDSPYXZGG7U27R5MBLGTNL",
    "QASMPRAKGNHURWBQMHQ3AS4W",
    "7VSO4FJ554XGZ4N6FJGVQKIS",
    "SY5ZXOWELPVTRJXWVGKONTNQ",
    "SQCPWERCWTU474FEHMMIIYM2",
    "FNIWKSCSZNNEEHAGTE32MZE6",
    "X7SFVU54U4M7N2CS7TP3SEMY",
    "CQMG7K5K3ZW6KAZOVQUS5TNP",
    "JCWALE6KIFW74RRWGSIZZMTG",
]

print("Cancelling 27 BR tasks on ee-whisp...")
for tid in br_ids:
    try:
        ee.data.cancelTask(tid)
    except Exception as e:
        print(f"  {tid}: {e}")
print("Done cancelling.")

# Now re-export under ee-andyarnellgee
import importlib
import openforis_whisp.export_cog as ec

# Re-initialize with correct project
ee.Initialize(project="ee-andyarnellgee")

print("\nRe-exporting BR under ee-andyarnellgee...")
results = ec.export_country_by_admin(iso2_codes=["BR"], auto_start=True)
print(f"\n=== Started {len(results)} tasks ===")
task_ids = []
for gaul_code, name, task in results:
    tid = task.id
    task_ids.append(tid)
    print(f"  GAUL {gaul_code} ({name}): {tid}")
print(f"\nTask IDs list:")
print(task_ids)
