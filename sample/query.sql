/*
Jednoduchy SQL dotaz se dvema WITH bloky pro predvedeni funkcnosti skriptu
sql2xml.py. Ucelem SQL dotazu je zjistit:
  (1) jak byla v jednotlivych letech ROK_OD az ROK_DO celkove hodnocena kvalita
      vyuky kurzu "Linearni algebra I" (zkratka "MLA1") a "Linearni algebra II"
      (zkratka "MLA2"),
  (2) kolik studentu melo v letech z kroku 1 zapsany oba zminene kurzy zaroven
      (tzn. v temze akademickem roce),
  (3) kolik z nich bylo v MLA1 hodnoceno lepe nez znamkou F,
  (4) kolik z nich bylo v MLA2 hodnoceno lepe nez znamkou F a
  (5) kolik z nich bylo hodnoceno znamkou lepsi nez F v obou uvedenych
      predmetech.
*/
WITH
    ---------- Predmety MLA1 a MLA2 s hodnocenim vyuky v ak. letech ROK_OD az ROK_DO
    -- Pozor: vyuziva bindovane promenne!
    w_mla AS (
        SELECT mla.zkr AS zkratka,
               mla.nazev,
               mla.p_id,     -- ID predmetu
               v.ak_rok,
               v.hodn_vyuky  -- Celkove hodnoceni vyuky predmetu v danem ak. roce
        FROM ( SELECT *
               FROM vyuka.predmet
               WHERE predmet.zkr IN ('MLA1', 'MLA2')  -- Jen predmety MLA1 a MLA2
             ) mla
             INNER JOIN vyuka.vyucovani v
                   ON ( v.p_id = mla.p_id
                        AND v.ak_rok BETWEEN :ROK_OD AND :ROK_DO  -- Jen s exist. hodnocenim vyuky v ak. letech ROK_OD az ROK_DO
                      )
    )  -- AS w_mla

    ---------- Hodnoceni studentu
    , w_zn AS (
        SELECT mla1.ak_rok,
               mla1.zkr AS zkr1,
               h1.zn_ects AS znamka1,  -- ECTS hodnoceni (A--F)
               mla2.zkr AS zkr2,
               h2.zn_ects AS znamka2,  -- ECTS hodnoceni (A--F)
               h1.s_id,                -- ID studenta
               DECODE(h1.zn_ects, 'F', 0, NULL, 0, 1) AS mla1_lepsi_nez_f,
               mla1.hodn_vyuky AS celk_hodn_vyuky_mla1,
               DECODE(h2.zn_ects, 'F', 0, NULL, 0, 1) AS mla2_lepsi_nez_f,
               mla2.hodn_vyuky AS celk_hodn_vyuky_mla2,
               DECODE(h1.zn_ects, 'F', 0, NULL, 0, 1)
                   * DECODE(h2.zn_ects, 'F', 0, NULL, 0, 1) AS oba_lepsi_nez_f
        FROM w_mla mla1
            INNER JOIN student.hodnoceni h1
                  ON ( h1.p_id = mla1.p_id
                       AND h1.ak_rok = mla1.ak_rok
                       AND mla1.zkr = 'MLA1'          -- Hodnoceni studenta v kurzu MLA1
                     )
            INNER JOIN w_mla mla2
                  ON ( mla2.zkr = 'MLA2'
                       AND mla2.ak_rok = mla1.ak_rok  -- MLA1 i MLA2 ve stejnem ak. roce
                     )
            INNER JOIN student.hodnoceni h2
                  ON ( h2.p_id = mla2.p_id
                       AND h2.ak_rok = mla2.ak_rok
                       AND h2.s_id = h1.s_id          -- Tentyz student, hodnoceni v MLA2
                     )
    )  -- AS w_zn

---------- Hlavni tabulka
SELECT zn.ak_rok AS rok,
       COUNT(1) celkem_studentu,
       SUM(zn.mla1_lepsi_nez_f) AS mla1_lepsi_nez_f,
       zn.celk_hodn_vyuky_mla1,
       SUM(zn.mla2_lepsi_nez_f) AS mla2_lepsi_nez_f,
       zn.celk_hodn_vyuky_mla2,
       SUM(zn.oba_lepsi_nez_f) AS oba_lepsi_nez_f
FROM w_zn zn
ORDER BY zn.ak_rok ASC
GROUP BY zn.ak_rok
