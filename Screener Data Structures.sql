-- Table: public.company_list

-- DROP TABLE public.company_list;

CREATE TABLE public.company_list
(
    sno character varying(5) COLLATE pg_catalog."default",
    name character varying(300) COLLATE pg_catalog."default",
    cmp money,
    sales money,
    sales_ann money,
    sales_prev_ann money,
    sales_var_3yrs_perc money,
    sales_var_5yrs_perc money,
    opm_perc money,
    opm_qtr_perc money,
    opm_ann_perc money,
    opm_prev_ann_perc money,
    pat_12m money,
    eps_12m money,
    eps_prev_ann money,
    pat_ann money,
    np_12m money,
    np_prev_ann money,
    npm_prev_ann_perc money,
    npm_ann_perc money,
    profit_var_5yrs_perc money,
    gross_block money,
    net_block money,
    cwip money,
    roce_perc money,
    investments money,
    cur_assets money,
    cur_liab money,
    contingent_liab money,
    profit_var_3yrs_perc money,
    chg_in_prom_hold_3yr_perc money,
    roa_12m_perc money,
    cmp_bv money,
    debt_eq money,
    roe_perc money,
    earnings_yield_perc money,
    pledged_perc money,
    ind_pe money,
    g_factor money,
    quick_rat money,
    roic_perc money,
    ind_pbv money,
    wk52_high money,
    roa_3yr_perc money,
    roce_3yr_perc money,
    roce_5yr_perc money,
    inventory_turnover_3yr money,
    roe_3yr_perc money,
    roe_5yr_perc money,
    annual_free_cash_flow_3yrs money,
    free_cash_flow_5yrs money,
    bv money,
    company_url character varying(2000) COLLATE pg_catalog."default",
    screen_id bigint
)

TABLESPACE pg_default;

ALTER TABLE public.company_list
    OWNER to postgres;
COMMENT ON TABLE public.company_list
    IS 'to synchronize with the screens';

-- Table: public.screens

-- DROP TABLE public.screens;

CREATE TABLE public.screens
(
    screen_id bigint NOT NULL,
    url character varying(1000) COLLATE pg_catalog."default" NOT NULL,
    query character varying(10000) COLLATE pg_catalog."default",
    CONSTRAINT listscreens_pkey PRIMARY KEY (screen_id),
    CONSTRAINT unq_url UNIQUE (url)
        INCLUDE(query)
)

TABLESPACE pg_default;

ALTER TABLE public.screens
    OWNER to postgres;
COMMENT ON TABLE public.screens
    IS 'Changed this to v1 - to ensure we dont duplicate the values or loose them
there is no unique index on the query column - hence too many duplicates';

COMMENT ON CONSTRAINT unq_url ON public.screens
    IS 'if the url + query combination is not unique then it will not be recorded';