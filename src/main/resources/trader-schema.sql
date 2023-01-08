--
-- PostgreSQL database dump
--

-- Dumped from database version 14.5
-- Dumped by pg_dump version 14.5

-- Started on 2023-01-08 19:25:42

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- TOC entry 232 (class 1255 OID 36510)
-- Name: add(integer, integer); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.add(integer, integer) RETURNS integer
    LANGUAGE sql IMMUTABLE STRICT
    AS $_$select $1 + $2;$_$;


ALTER FUNCTION public.add(integer, integer) OWNER TO postgres;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- TOC entry 209 (class 1259 OID 36184)
-- Name: address; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.address (
    row_id bigint NOT NULL,
    dc_address character varying NOT NULL
);


ALTER TABLE public.address OWNER TO postgres;

--
-- TOC entry 210 (class 1259 OID 36189)
-- Name: address_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.address_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.address_id_seq OWNER TO postgres;

--
-- TOC entry 3489 (class 0 OID 0)
-- Dependencies: 210
-- Name: address_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.address_id_seq OWNED BY public.address.row_id;


--
-- TOC entry 211 (class 1259 OID 36190)
-- Name: auction_bid; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.auction_bid (
    tx_row_id bigint NOT NULL,
    token integer NOT NULL,
    amount numeric(20,8) NOT NULL,
    vault bigint NOT NULL,
    index integer NOT NULL,
    owner bigint NOT NULL
);


ALTER TABLE public.auction_bid OWNER TO postgres;

--
-- TOC entry 212 (class 1259 OID 36193)
-- Name: block; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.block (
    height integer NOT NULL,
    "time" bigint NOT NULL,
    hash character(64) NOT NULL,
    finalized boolean DEFAULT false NOT NULL,
    master_node bigint,
    minter bigint
);


ALTER TABLE public.block OWNER TO postgres;

--
-- TOC entry 213 (class 1259 OID 36197)
-- Name: blocks_not_finalized; Type: VIEW; Schema: public; Owner: postgres
--

CREATE VIEW public.blocks_not_finalized AS
 SELECT block.height
   FROM public.block
  WHERE (block.finalized = false)
  ORDER BY block.height DESC
 LIMIT 1000;


ALTER TABLE public.blocks_not_finalized OWNER TO postgres;

--
-- TOC entry 214 (class 1259 OID 36201)
-- Name: collateral; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.collateral (
    tx_row_id bigint NOT NULL,
    vault bigint NOT NULL,
    owner bigint NOT NULL
);


ALTER TABLE public.collateral OWNER TO postgres;

--
-- TOC entry 215 (class 1259 OID 36204)
-- Name: fee; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.fee (
    in_a numeric(20,8),
    out_a numeric(20,8),
    in_b numeric(20,8),
    out_b numeric(20,8),
    commission numeric(20,8),
    block_height integer NOT NULL,
    token integer NOT NULL
);


ALTER TABLE public.fee OWNER TO postgres;

--
-- TOC entry 216 (class 1259 OID 36207)
-- Name: loan; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.loan (
    tx_row_id bigint NOT NULL,
    vault bigint NOT NULL,
    owner bigint NOT NULL
);


ALTER TABLE public.loan OWNER TO postgres;

--
-- TOC entry 217 (class 1259 OID 36210)
-- Name: mempool; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.mempool (
    tx_row_id bigint NOT NULL,
    block_height integer NOT NULL,
    "time" bigint NOT NULL,
    txn integer NOT NULL
);


ALTER TABLE public.mempool OWNER TO postgres;

--
-- TOC entry 218 (class 1259 OID 36213)
-- Name: minted_tx; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.minted_tx (
    block_height integer NOT NULL,
    txn integer NOT NULL,
    tx_row_id bigint NOT NULL
);


ALTER TABLE public.minted_tx OWNER TO postgres;

--
-- TOC entry 219 (class 1259 OID 36216)
-- Name: missing_blocks; Type: VIEW; Schema: public; Owner: postgres
--

CREATE VIEW public.missing_blocks AS
 WITH first_missing AS (
         SELECT ( SELECT (max(a.height) - 1)
                   FROM public.block a
                  WHERE (NOT (EXISTS ( SELECT 1
                           FROM public.block b
                          WHERE (b.height = (a.height - 1)))))) AS block_height
        )
 SELECT generate_series((first_missing.block_height)::bigint, GREATEST(((first_missing.block_height - 1000))::bigint, (0)::bigint), ('-1'::integer)::bigint) AS missing_block
   FROM first_missing
EXCEPT
 SELECT block.height AS missing_block
   FROM public.block,
    first_missing
  WHERE ((block.height <= first_missing.block_height) AND (block.height >= (first_missing.block_height - 1000)))
  ORDER BY 1 DESC;


ALTER TABLE public.missing_blocks OWNER TO postgres;

--
-- TOC entry 220 (class 1259 OID 36221)
-- Name: oracle_price; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.oracle_price (
    token integer NOT NULL,
    price numeric(20,8) NOT NULL,
    block_height integer NOT NULL
);


ALTER TABLE public.oracle_price OWNER TO postgres;

--
-- TOC entry 221 (class 1259 OID 36224)
-- Name: pool_liquidity; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.pool_liquidity (
    tx_row_id bigint NOT NULL,
    token_a integer NOT NULL,
    token_b integer NOT NULL,
    amount_a numeric(20,8),
    amount_b numeric(20,8),
    pool integer NOT NULL,
    owner bigint NOT NULL,
    shares numeric(20,8)
);


ALTER TABLE public.pool_liquidity OWNER TO postgres;

--
-- TOC entry 222 (class 1259 OID 36227)
-- Name: pool_pair; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.pool_pair (
    reserve_a numeric(20,8) NOT NULL,
    reserve_b numeric(20,8) NOT NULL,
    token integer NOT NULL,
    block_height integer NOT NULL
);


ALTER TABLE public.pool_pair OWNER TO postgres;

--
-- TOC entry 223 (class 1259 OID 36230)
-- Name: pool_swap; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.pool_swap (
    tx_row_id bigint NOT NULL,
    "from" bigint NOT NULL,
    "to" bigint NOT NULL,
    token_from integer NOT NULL,
    token_to integer NOT NULL,
    amount_from numeric(20,8) NOT NULL,
    amount_to numeric(20,8) NOT NULL,
    max_price numeric(20,8) NOT NULL,
    token_to_alt integer NOT NULL
);


ALTER TABLE public.pool_swap OWNER TO postgres;

--
-- TOC entry 224 (class 1259 OID 36233)
-- Name: token; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.token (
    dc_token_id integer NOT NULL,
    dc_token_symbol character varying NOT NULL
);


ALTER TABLE public.token OWNER TO postgres;

--
-- TOC entry 225 (class 1259 OID 36238)
-- Name: token_amount; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.token_amount (
    tx_row_id bigint NOT NULL,
    token integer NOT NULL,
    amount numeric(20,8) NOT NULL
);


ALTER TABLE public.token_amount OWNER TO postgres;

--
-- TOC entry 226 (class 1259 OID 36241)
-- Name: tx; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.tx (
    row_id bigint NOT NULL,
    dc_tx_id character(64) NOT NULL,
    tx_type_row_id integer NOT NULL,
    confirmed boolean NOT NULL,
    fee numeric(20,8) DEFAULT 0.0 NOT NULL,
    valid boolean DEFAULT true NOT NULL,
    size integer DEFAULT 0 NOT NULL,
    fee_rate numeric(20,8) DEFAULT 0.0 NOT NULL
);


ALTER TABLE public.tx OWNER TO postgres;

--
-- TOC entry 227 (class 1259 OID 36248)
-- Name: tx_row_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.tx_row_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.tx_row_id_seq OWNER TO postgres;

--
-- TOC entry 3490 (class 0 OID 0)
-- Dependencies: 227
-- Name: tx_row_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.tx_row_id_seq OWNED BY public.tx.row_id;


--
-- TOC entry 228 (class 1259 OID 36249)
-- Name: tx_type; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.tx_type (
    row_id integer NOT NULL,
    dc_tx_type character varying NOT NULL
);


ALTER TABLE public.tx_type OWNER TO postgres;

--
-- TOC entry 229 (class 1259 OID 36254)
-- Name: tx_type_row_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.tx_type_row_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.tx_type_row_id_seq OWNER TO postgres;

--
-- TOC entry 3491 (class 0 OID 0)
-- Dependencies: 229
-- Name: tx_type_row_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.tx_type_row_id_seq OWNED BY public.tx_type.row_id;


--
-- TOC entry 230 (class 1259 OID 36255)
-- Name: vault; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.vault (
    row_id bigint NOT NULL,
    dc_vault_id character(64) NOT NULL,
    owner bigint
);


ALTER TABLE public.vault OWNER TO postgres;

--
-- TOC entry 231 (class 1259 OID 36258)
-- Name: vault_row_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.vault_row_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.vault_row_id_seq OWNER TO postgres;

--
-- TOC entry 3492 (class 0 OID 0)
-- Dependencies: 231
-- Name: vault_row_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.vault_row_id_seq OWNED BY public.vault.row_id;


--
-- TOC entry 3240 (class 2604 OID 36259)
-- Name: address row_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.address ALTER COLUMN row_id SET DEFAULT nextval('public.address_id_seq'::regclass);


--
-- TOC entry 3246 (class 2604 OID 36260)
-- Name: tx row_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.tx ALTER COLUMN row_id SET DEFAULT nextval('public.tx_row_id_seq'::regclass);


--
-- TOC entry 3247 (class 2604 OID 36261)
-- Name: tx_type row_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.tx_type ALTER COLUMN row_id SET DEFAULT nextval('public.tx_type_row_id_seq'::regclass);


--
-- TOC entry 3248 (class 2604 OID 36262)
-- Name: vault row_id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.vault ALTER COLUMN row_id SET DEFAULT nextval('public.vault_row_id_seq'::regclass);


--
-- TOC entry 3250 (class 2606 OID 36266)
-- Name: address address_name_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.address
    ADD CONSTRAINT address_name_key UNIQUE (dc_address);


--
-- TOC entry 3252 (class 2606 OID 36268)
-- Name: address address_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.address
    ADD CONSTRAINT address_pkey PRIMARY KEY (row_id);


--
-- TOC entry 3254 (class 2606 OID 36270)
-- Name: auction_bid auction_bid_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.auction_bid
    ADD CONSTRAINT auction_bid_pkey PRIMARY KEY (tx_row_id);


--
-- TOC entry 3256 (class 2606 OID 36272)
-- Name: block block_hash_unique; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.block
    ADD CONSTRAINT block_hash_unique UNIQUE (hash);


--
-- TOC entry 3258 (class 2606 OID 36274)
-- Name: block block_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.block
    ADD CONSTRAINT block_pkey PRIMARY KEY (height);


--
-- TOC entry 3260 (class 2606 OID 36276)
-- Name: collateral collateral_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.collateral
    ADD CONSTRAINT collateral_pkey PRIMARY KEY (tx_row_id);


--
-- TOC entry 3262 (class 2606 OID 36278)
-- Name: fee fee_token_block_height_unique; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.fee
    ADD CONSTRAINT fee_token_block_height_unique UNIQUE (block_height, token);


--
-- TOC entry 3264 (class 2606 OID 36280)
-- Name: loan loan_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.loan
    ADD CONSTRAINT loan_pkey PRIMARY KEY (tx_row_id);


--
-- TOC entry 3268 (class 2606 OID 36282)
-- Name: mempool mempool_block_height_txn_unique; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.mempool
    ADD CONSTRAINT mempool_block_height_txn_unique UNIQUE (block_height, txn);


--
-- TOC entry 3270 (class 2606 OID 36284)
-- Name: mempool mempool_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.mempool
    ADD CONSTRAINT mempool_pkey PRIMARY KEY (tx_row_id);


--
-- TOC entry 3274 (class 2606 OID 36286)
-- Name: minted_tx minted_tx_block_height_index_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.minted_tx
    ADD CONSTRAINT minted_tx_block_height_index_key UNIQUE (block_height, txn);


--
-- TOC entry 3277 (class 2606 OID 36288)
-- Name: minted_tx minted_tx_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.minted_tx
    ADD CONSTRAINT minted_tx_pkey PRIMARY KEY (tx_row_id);


--
-- TOC entry 3279 (class 2606 OID 36290)
-- Name: oracle_price oracle_price_token_block_height_unique; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.oracle_price
    ADD CONSTRAINT oracle_price_token_block_height_unique UNIQUE (block_height, token);


--
-- TOC entry 3283 (class 2606 OID 36292)
-- Name: pool_liquidity pool_liquidity_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.pool_liquidity
    ADD CONSTRAINT pool_liquidity_pkey PRIMARY KEY (tx_row_id);


--
-- TOC entry 3285 (class 2606 OID 36294)
-- Name: pool_pair pool_pair_token_block_height_unique; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.pool_pair
    ADD CONSTRAINT pool_pair_token_block_height_unique UNIQUE (token, block_height);


--
-- TOC entry 3290 (class 2606 OID 36296)
-- Name: pool_swap pool_swap_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.pool_swap
    ADD CONSTRAINT pool_swap_pkey PRIMARY KEY (tx_row_id);


--
-- TOC entry 3292 (class 2606 OID 36298)
-- Name: token token_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.token
    ADD CONSTRAINT token_pkey PRIMARY KEY (dc_token_id);


--
-- TOC entry 3295 (class 2606 OID 36300)
-- Name: tx tx_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.tx
    ADD CONSTRAINT tx_pkey PRIMARY KEY (row_id);


--
-- TOC entry 3297 (class 2606 OID 36302)
-- Name: tx tx_tx_id_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.tx
    ADD CONSTRAINT tx_tx_id_key UNIQUE (dc_tx_id);


--
-- TOC entry 3299 (class 2606 OID 36304)
-- Name: tx_type tx_type_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.tx_type
    ADD CONSTRAINT tx_type_pkey PRIMARY KEY (row_id);


--
-- TOC entry 3301 (class 2606 OID 36306)
-- Name: tx_type tx_type_type_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.tx_type
    ADD CONSTRAINT tx_type_type_key UNIQUE (dc_tx_type);


--
-- TOC entry 3303 (class 2606 OID 36308)
-- Name: vault vault_dc_vault_id_key; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.vault
    ADD CONSTRAINT vault_dc_vault_id_key UNIQUE (dc_vault_id);


--
-- TOC entry 3305 (class 2606 OID 36310)
-- Name: vault vault_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.vault
    ADD CONSTRAINT vault_pkey PRIMARY KEY (row_id);


--
-- TOC entry 3280 (class 1259 OID 36311)
-- Name: amount_a_btree; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX amount_a_btree ON public.pool_liquidity USING btree (amount_a);


--
-- TOC entry 3281 (class 1259 OID 36312)
-- Name: amount_b_btree; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX amount_b_btree ON public.pool_liquidity USING btree (amount_b);


--
-- TOC entry 3265 (class 1259 OID 36313)
-- Name: fki_mempool_tx_id_fkey; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX fki_mempool_tx_id_fkey ON public.mempool USING btree (tx_row_id);


--
-- TOC entry 3272 (class 1259 OID 36314)
-- Name: fki_minted_tx_tx_id_fkey; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX fki_minted_tx_tx_id_fkey ON public.minted_tx USING btree (tx_row_id);


--
-- TOC entry 3286 (class 1259 OID 36315)
-- Name: fki_pool_swap_tx_id_fkey; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX fki_pool_swap_tx_id_fkey ON public.pool_swap USING btree (tx_row_id);


--
-- TOC entry 3293 (class 1259 OID 36316)
-- Name: fki_tx_type_fkey; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX fki_tx_type_fkey ON public.tx USING btree (tx_type_row_id);


--
-- TOC entry 3266 (class 1259 OID 36318)
-- Name: mempool_block_height_txn_bkey; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX mempool_block_height_txn_bkey ON public.mempool USING btree (block_height DESC NULLS LAST, txn);


--
-- TOC entry 3271 (class 1259 OID 36319)
-- Name: mempool_time_bkey; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX mempool_time_bkey ON public.mempool USING btree ("time" DESC NULLS LAST);


--
-- TOC entry 3275 (class 1259 OID 36320)
-- Name: minted_tx_block_height_txn_bkey; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX minted_tx_block_height_txn_bkey ON public.minted_tx USING btree (block_height DESC NULLS LAST, txn);


--
-- TOC entry 3287 (class 1259 OID 36321)
-- Name: pool_swap_amount_from_btree; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX pool_swap_amount_from_btree ON public.pool_swap USING btree (amount_from);


--
-- TOC entry 3288 (class 1259 OID 36322)
-- Name: pool_swap_amount_to_btree; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX pool_swap_amount_to_btree ON public.pool_swap USING btree (amount_to);


--
-- TOC entry 3306 (class 2606 OID 36323)
-- Name: auction_bid auction_bid_owner_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.auction_bid
    ADD CONSTRAINT auction_bid_owner_fkey FOREIGN KEY (owner) REFERENCES public.address(row_id) ON UPDATE CASCADE ON DELETE CASCADE NOT VALID;


--
-- TOC entry 3307 (class 2606 OID 36328)
-- Name: auction_bid auction_bid_token_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.auction_bid
    ADD CONSTRAINT auction_bid_token_fkey FOREIGN KEY (token) REFERENCES public.token(dc_token_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3308 (class 2606 OID 36333)
-- Name: auction_bid auction_bid_tx_row_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.auction_bid
    ADD CONSTRAINT auction_bid_tx_row_id_fkey FOREIGN KEY (tx_row_id) REFERENCES public.tx(row_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3309 (class 2606 OID 36338)
-- Name: auction_bid auction_bid_vault_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.auction_bid
    ADD CONSTRAINT auction_bid_vault_fkey FOREIGN KEY (vault) REFERENCES public.vault(row_id) ON UPDATE CASCADE ON DELETE CASCADE NOT VALID;


--
-- TOC entry 3310 (class 2606 OID 36343)
-- Name: block block_master_node_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.block
    ADD CONSTRAINT block_master_node_fkey FOREIGN KEY (master_node) REFERENCES public.tx(row_id) ON UPDATE CASCADE ON DELETE CASCADE NOT VALID;


--
-- TOC entry 3311 (class 2606 OID 36348)
-- Name: block block_minter_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.block
    ADD CONSTRAINT block_minter_fkey FOREIGN KEY (minter) REFERENCES public.address(row_id) ON UPDATE CASCADE ON DELETE CASCADE NOT VALID;


--
-- TOC entry 3312 (class 2606 OID 36353)
-- Name: collateral collateral_from_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.collateral
    ADD CONSTRAINT collateral_from_fkey FOREIGN KEY (owner) REFERENCES public.address(row_id) ON UPDATE CASCADE ON DELETE CASCADE NOT VALID;


--
-- TOC entry 3313 (class 2606 OID 36358)
-- Name: collateral collateral_tx_row_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.collateral
    ADD CONSTRAINT collateral_tx_row_id_fkey FOREIGN KEY (tx_row_id) REFERENCES public.tx(row_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3314 (class 2606 OID 36363)
-- Name: collateral collateral_vault_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.collateral
    ADD CONSTRAINT collateral_vault_fkey FOREIGN KEY (vault) REFERENCES public.vault(row_id) ON UPDATE CASCADE ON DELETE CASCADE NOT VALID;


--
-- TOC entry 3315 (class 2606 OID 36368)
-- Name: fee fee_block_height_fk; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.fee
    ADD CONSTRAINT fee_block_height_fk FOREIGN KEY (block_height) REFERENCES public.block(height) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3316 (class 2606 OID 36373)
-- Name: fee fee_token_fk; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.fee
    ADD CONSTRAINT fee_token_fk FOREIGN KEY (token) REFERENCES public.token(dc_token_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3317 (class 2606 OID 36378)
-- Name: loan loan_owner_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.loan
    ADD CONSTRAINT loan_owner_fkey FOREIGN KEY (owner) REFERENCES public.address(row_id) ON UPDATE CASCADE ON DELETE CASCADE NOT VALID;


--
-- TOC entry 3318 (class 2606 OID 36383)
-- Name: loan loan_tx_row_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.loan
    ADD CONSTRAINT loan_tx_row_id_fkey FOREIGN KEY (tx_row_id) REFERENCES public.tx(row_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3319 (class 2606 OID 36388)
-- Name: loan loan_vault_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.loan
    ADD CONSTRAINT loan_vault_fkey FOREIGN KEY (vault) REFERENCES public.vault(row_id) ON UPDATE CASCADE ON DELETE CASCADE NOT VALID;


--
-- TOC entry 3320 (class 2606 OID 36393)
-- Name: mempool mempool_block_height_received_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.mempool
    ADD CONSTRAINT mempool_block_height_received_fkey FOREIGN KEY (block_height) REFERENCES public.block(height) ON UPDATE CASCADE ON DELETE CASCADE NOT VALID;


--
-- TOC entry 3321 (class 2606 OID 36398)
-- Name: mempool mempool_tx_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.mempool
    ADD CONSTRAINT mempool_tx_id_fkey FOREIGN KEY (tx_row_id) REFERENCES public.tx(row_id) MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3322 (class 2606 OID 36403)
-- Name: minted_tx minted_tx_block_height_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.minted_tx
    ADD CONSTRAINT minted_tx_block_height_fkey FOREIGN KEY (block_height) REFERENCES public.block(height) ON UPDATE CASCADE ON DELETE CASCADE NOT VALID;


--
-- TOC entry 3323 (class 2606 OID 36408)
-- Name: minted_tx minted_tx_tx_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.minted_tx
    ADD CONSTRAINT minted_tx_tx_id_fkey FOREIGN KEY (tx_row_id) REFERENCES public.tx(row_id) MATCH FULL ON UPDATE CASCADE ON DELETE CASCADE NOT VALID;


--
-- TOC entry 3324 (class 2606 OID 36413)
-- Name: oracle_price oracle_price_block_height_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.oracle_price
    ADD CONSTRAINT oracle_price_block_height_fkey FOREIGN KEY (block_height) REFERENCES public.block(height) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3325 (class 2606 OID 36418)
-- Name: oracle_price oracle_price_token_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.oracle_price
    ADD CONSTRAINT oracle_price_token_fkey FOREIGN KEY (token) REFERENCES public.token(dc_token_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3326 (class 2606 OID 36423)
-- Name: pool_liquidity pool_liquidity_owner_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.pool_liquidity
    ADD CONSTRAINT pool_liquidity_owner_fkey FOREIGN KEY (owner) REFERENCES public.address(row_id) ON UPDATE CASCADE ON DELETE CASCADE NOT VALID;


--
-- TOC entry 3327 (class 2606 OID 36428)
-- Name: pool_liquidity pool_liquidity_pool_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.pool_liquidity
    ADD CONSTRAINT pool_liquidity_pool_fkey FOREIGN KEY (pool) REFERENCES public.token(dc_token_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3328 (class 2606 OID 36433)
-- Name: pool_liquidity pool_liquidity_token_a_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.pool_liquidity
    ADD CONSTRAINT pool_liquidity_token_a_fkey FOREIGN KEY (token_a) REFERENCES public.token(dc_token_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3329 (class 2606 OID 36438)
-- Name: pool_liquidity pool_liquidity_token_b_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.pool_liquidity
    ADD CONSTRAINT pool_liquidity_token_b_fkey FOREIGN KEY (token_b) REFERENCES public.token(dc_token_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3330 (class 2606 OID 36443)
-- Name: pool_liquidity pool_liquidity_tx_row_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.pool_liquidity
    ADD CONSTRAINT pool_liquidity_tx_row_id_fkey FOREIGN KEY (tx_row_id) REFERENCES public.tx(row_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3331 (class 2606 OID 36448)
-- Name: pool_pair pool_pair_block_height_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.pool_pair
    ADD CONSTRAINT pool_pair_block_height_fkey FOREIGN KEY (block_height) REFERENCES public.block(height) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3332 (class 2606 OID 36453)
-- Name: pool_pair pool_pair_token_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.pool_pair
    ADD CONSTRAINT pool_pair_token_fkey FOREIGN KEY (token) REFERENCES public.token(dc_token_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3333 (class 2606 OID 36458)
-- Name: pool_swap pool_swap_from_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.pool_swap
    ADD CONSTRAINT pool_swap_from_fkey FOREIGN KEY ("from") REFERENCES public.address(row_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3334 (class 2606 OID 36463)
-- Name: pool_swap pool_swap_to_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.pool_swap
    ADD CONSTRAINT pool_swap_to_fkey FOREIGN KEY ("to") REFERENCES public.address(row_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3335 (class 2606 OID 36468)
-- Name: pool_swap pool_swap_token_from_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.pool_swap
    ADD CONSTRAINT pool_swap_token_from_fkey FOREIGN KEY (token_from) REFERENCES public.token(dc_token_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3336 (class 2606 OID 36473)
-- Name: pool_swap pool_swap_token_to_alt_fk; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.pool_swap
    ADD CONSTRAINT pool_swap_token_to_alt_fk FOREIGN KEY (token_to_alt) REFERENCES public.token(dc_token_id) ON UPDATE CASCADE ON DELETE CASCADE NOT VALID;


--
-- TOC entry 3337 (class 2606 OID 36478)
-- Name: pool_swap pool_swap_token_to_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.pool_swap
    ADD CONSTRAINT pool_swap_token_to_fkey FOREIGN KEY (token_to) REFERENCES public.token(dc_token_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3338 (class 2606 OID 36483)
-- Name: pool_swap pool_swap_tx_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.pool_swap
    ADD CONSTRAINT pool_swap_tx_id_fkey FOREIGN KEY (tx_row_id) REFERENCES public.tx(row_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3339 (class 2606 OID 36488)
-- Name: token_amount token_amount_token_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.token_amount
    ADD CONSTRAINT token_amount_token_fkey FOREIGN KEY (token) REFERENCES public.token(dc_token_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3340 (class 2606 OID 36493)
-- Name: token_amount token_amount_tx_row_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.token_amount
    ADD CONSTRAINT token_amount_tx_row_id_fkey FOREIGN KEY (tx_row_id) REFERENCES public.tx(row_id) ON UPDATE CASCADE ON DELETE CASCADE;


--
-- TOC entry 3341 (class 2606 OID 36498)
-- Name: tx tx_type_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.tx
    ADD CONSTRAINT tx_type_fkey FOREIGN KEY (tx_type_row_id) REFERENCES public.tx_type(row_id) ON UPDATE CASCADE ON DELETE CASCADE NOT VALID;


--
-- TOC entry 3342 (class 2606 OID 36503)
-- Name: vault vault_owner_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.vault
    ADD CONSTRAINT vault_owner_fkey FOREIGN KEY (owner) REFERENCES public.address(row_id) ON UPDATE CASCADE ON DELETE CASCADE;


-- Completed on 2023-01-08 19:25:42

--
-- PostgreSQL database dump complete
--

