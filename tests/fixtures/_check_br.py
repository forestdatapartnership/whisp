import ee

ee.Initialize(project="ee-andyarnellgee")
br_ids = [
    "6YRRJWDH2ZIOGKCX7JJSAJFF",
    "F65NHAKLIWTE246Y5CCGSENV",
    "JCZ5X5CFDKAHYFANADVVJAXY",
    "PXZH5R4XCLJZKTZ323JB624S",
    "YF6O4R33YUBG2D5MJNA3E3R3",
    "FJMDDCTZSKCDKATJ66V6JWBZ",
    "KLTTVJYRBG2KQ7WYVJMAG4VI",
    "MK6Q6XKWMTUC4VUGNNYJNB6E",
    "BQX6KEFHNSJUK4WYUH3V6XT3",
    "4HULJMOBEHTKMBQWXZVFINS3",
    "6A4T7Y3OPBZNAYOPN62NJE5H",
    "L2DCG7OBSBJVZKSIZ3TI7X4Q",
    "XO6BDZMRMI2BQE5AK6ZUIE7S",
    "B5LXBRBJ35DNVPPLJ2YM5AJL",
    "ITHPEEYWZ5R3BTN4IQDDBTZE",
    "AXJUJH2YCD3UQU3V4A7JMUR3",
    "O4IFYLQOXFDDQZ3PJYJFAAAN",
    "MXWKUDKXFWHID5SCUDSSI5QS",
    "NERFORIOGKMHEOL3M32XFXOZ",
    "SP4DMII5F3LODZSCXGOEU6EF",
    "WSQR3O37PL5IEDYKXURU4VOO",
    "7INUIUOT7ZRUMS3JJ3GAGRZL",
    "4V2UUFMGMZZBOIQX7D4BNKAQ",
    "T2LXAM5BDYOJFQDBK3FQ5R57",
    "ZAMWGY5QPAA6Z7GXEL5NLH4G",
    "XYJLM235IRNK2JUCDYD2MKJX",
    "42ABMOF5R6LJ4OWTRTJCPFUB",
]
names = [
    "Acre",
    "Alagoas",
    "Amapá",
    "Amazonas",
    "Bahia",
    "Ceará",
    "Distrito Federal",
    "Espírito Santo",
    "Goiás",
    "Maranhão",
    "Mato Grosso",
    "Mato Grosso Do Sul",
    "Minas Gerais",
    "Paraná",
    "Paraíba",
    "Pará",
    "Pernambuco",
    "Piauí",
    "Rio De Janeiro",
    "Rio Grande Do Norte",
    "Rio Grande Do Sul",
    "Rondônia",
    "Roraima",
    "Santa Catarina",
    "Sergipe",
    "São Paulo",
    "Tocantins",
]
statuses = ee.data.getTaskStatus(br_ids)
counts = {}
for s, n in zip(statuses, names):
    st = s["state"]
    counts[st] = counts.get(st, 0) + 1
    extra = ""
    if st == "RUNNING" and "progress" in s:
        extra = f" ({s['progress']:.0%})"
    print(f"  {st:10s}{extra}  {n}")
print()
for st, c in sorted(counts.items()):
    print(f"{st}: {c}")
print(f"Total: {len(statuses)}")
