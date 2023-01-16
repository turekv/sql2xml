/* Programy - garanti - rozliseni, kolik programu garant ma
   Sloucit programy (aby pouze zmena jazyka nebo formy neznamenala zdvojeni)
*/
with
    --------------------------- Garanti a sloucene programy (pouze pokud se lisi typem a nazvem)
    program_tmp as (select distinct pe.*
            , cts.zkratka as typ
            , pg.nazev as program
            , pg.nazev || ' (' || cts.zkratka || ')' as typ_program
        from st01.program pg                                                            -- *** Program
            inner join st01.c_typ_studia cts                                            -- *** C_typ_studia
                  on ( cts.typ_studia_id = pg.typ_studia_id
                       -- and cts.status = 9 
                       and pg.fakulta_id = 4
                       and pg.rok_platnosti = 2022      -- !!! Rok aktualni
                       and pg.akreditace_do >= to_date('2025-01-01', 'YYYY-MM-DD')
                       and pg.status = 9
                      )
            inner join st01.c_forma_studia cfs                                          -- *** C_forma_studia
                  on ( cfs.forma_studia_id = pg.forma_studia_id
                       and cfs.zkratka in ('P', 'K')
                       -- and cfs.status = 9 
                      )
            inner join (select pe50.per_id --, pe50.per_family_names as prijmeni, pe50.per_first_names as jmeno 
                        , pe50.label_pr as garant
                                   from brutisadm.person pe50                           -- *** Person
                                   where pe50.status = 9 ) pe
                   on pe.per_id = pg.garant_id
    ) -- as program_tmp     --------------------------- Garanti a sloucene programy (pouze pokud se lisi typem a nazvem)
    
--select tmp.* from program_tmp tmp 
--/*


    ---------------------------
    , garant_tmp as (select pt.per_id, pt.garant
            , count(distinct pt.typ_program) as pocet_programu
            , listagg(distinct pt.typ,', ') within group(order by pt.typ) as typy_programu
            , listagg(pt.typ_program,', ') within group(order by pt.typ_program) as programy
        from program_tmp pt                                                             -- *** Program_tmp
        group by pt.per_id, pt.garant
    ) -- as garant_tmp     ---------------------------
    
select tmp.* from garant_tmp tmp
order by pocet_programu, garant
/*

-- */