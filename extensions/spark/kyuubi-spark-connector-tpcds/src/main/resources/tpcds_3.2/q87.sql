--
-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
--    http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--
-- q87 --
select
     count(*)
from
     (
          (
               select
                    distinct c_last_name,
                    c_first_name,
                    d_date
               from
                    store_sales,
                    date_dim,
                    customer
               where
                    store_sales.ss_sold_date_sk = date_dim.d_date_sk
                    and store_sales.ss_customer_sk = customer.c_customer_sk
                    and d_month_seq between 1212
                    and 1212 + 11
          )
          except
               (
                    select
                         distinct c_last_name,
                         c_first_name,
                         d_date
                    from
                         catalog_sales,
                         date_dim,
                         customer
                    where
                         catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
                         and catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
                         and d_month_seq between 1212
                         and 1212 + 11
               )
          except
               (
                    select
                         distinct c_last_name,
                         c_first_name,
                         d_date
                    from
                         web_sales,
                         date_dim,
                         customer
                    where
                         web_sales.ws_sold_date_sk = date_dim.d_date_sk
                         and web_sales.ws_bill_customer_sk = customer.c_customer_sk
                         and d_month_seq between 1212
                         and 1212 + 11
               )
     ) cool_cust;
