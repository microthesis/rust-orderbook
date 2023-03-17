use rand::Rng;
use rayon::prelude::*;
use std::collections::{BTreeMap, HashMap, VecDeque};

#[derive(Debug)]
pub enum Side {
    Bid,
    Ask,
}

#[derive(Debug)]
pub enum OrderStatus {
    Uninitialized,
    Created,
    Filled,
    PartiallyFilled,
    Stale,
}

#[derive(Debug)]
pub struct FillResult {
    pub filled_orders: Vec<(u64, u64)>,
    pub remaining_quantity: u64,
    pub status: OrderStatus,
}

impl FillResult {
    fn new() -> Self {
        FillResult {
            filled_orders: Vec::new(),
            remaining_quantity: u64::MAX,
            status: OrderStatus::Uninitialized,
        }
    }

    pub fn avg_fill_price(&self) -> f32 {
        let mut total_price_paid = 0;
        let mut total_q = 0;

        for (q, p) in &self.filled_orders {
            total_price_paid += q * p;
            total_q += q;
        }

        total_price_paid as f32 / total_q as f32
    }
}

#[derive(Debug)]
struct Order {
    pub id: u64,
    pub quantity: u64,
}

#[derive(Debug)]
struct OrderBookSide {
    side: Side,
    price_map: BTreeMap<u64, usize>,
    price_levels: Vec<VecDeque<Order>>,
}
impl OrderBookSide {
    pub fn new(s: Side) -> Self {
        OrderBookSide {
            side: s,
            price_map: BTreeMap::new(),
            price_levels: Vec::with_capacity(50_000),
        }
    }

    pub fn get_total_quantity(&self, price: u64) -> u64 {
        self.price_levels[self.price_map[&price]]
            .par_iter()
            .map(|x| x.quantity)
            .sum()
    }
}

#[derive(Debug)]
pub struct OrderBook {
    symbol: String,
    bb: Option<u64>,
    ba: Option<u64>,
    bids: OrderBookSide,
    asks: OrderBookSide,

    order_level: HashMap<u64, (Side, usize)>,
}

impl OrderBook {
    pub fn new(symbol: String) -> Self {
        OrderBook {
            symbol,
            bb: None,
            ba: None,
            bids: OrderBookSide::new(Side::Bid),
            asks: OrderBookSide::new(Side::Ask),
            order_level: HashMap::with_capacity(50_000),
        }
    }

    pub fn cancel_order(&mut self, order_id: u64) -> Result<&str, &str> {
        if let Some((side, price_level)) = self.order_level.get(&order_id) {
            let level_queue = match side {
                Side::Bid => self.bids.price_levels.get_mut(*price_level).unwrap(),
                Side::Ask => self.asks.price_levels.get_mut(*price_level).unwrap(),
            };
            level_queue.retain(|x| x.id != order_id);
            self.order_level.remove(&order_id);
            Ok("Cancelled order")
        } else {
            Err("Order id not found")
        }
    }

    fn create_new_limit_order(&mut self, s: Side, price: u64, q: u64) -> u64 {
        let mut rng = rand::thread_rng();
        let order_id: u64 = rng.gen();
        let book = match s {
            Side::Ask => &mut self.asks,
            Side::Bid => &mut self.bids,
        };
        let order = Order {
            id: order_id,
            quantity: q,
        };

        if let Some(val) = book.price_map.get(&price) {
            book.price_levels[*val].push_back(order);
            self.order_level.insert(order_id, (s, *val));
        } else {
            let new_level = book.price_levels.len();
            book.price_map.insert(price, new_level);
            let mut deque = VecDeque::new();
            deque.push_back(order);
            book.price_levels.push(deque);
            self.order_level.insert(order_id, (s, new_level));
        }

        order_id
    }

    fn update_best_bid_and_ask(&mut self) {
        for (p, u) in self.bids.price_map.iter().rev() {
            if !self.bids.price_levels[*u].is_empty() {
                self.bb = Some(*p);
                break;
            }
        }
        for (p, u) in self.asks.price_map.iter() {
            if !self.asks.price_levels[*u].is_empty() {
                self.ba = Some(*p);
                break;
            }
        }
    }

    pub fn bbo(&self) {
        if self.bb.is_some() & self.ba.is_some() {
            let bb = self.bb.unwrap();
            let ba = self.ba.unwrap();

            let total_bid = self.bids.get_total_quantity(bb);
            let total_ask = self.asks.get_total_quantity(ba);

            println!("Best bid {}, qty {}", bb, total_bid);
            println!("Best ask {}, qty {}", ba, total_ask);

            let spread = ((ba - bb) as f64 / ba as f64) as f32;

            println!("Spread is {:.6},", spread);
        }
    }

    pub fn add_limit_order(&mut self, s: Side, price: u64, order_q: u64) -> FillResult {
        fn match_at_price_level(
            price_level: &mut VecDeque<Order>,
            incoming_order_qty: &mut u64,
            order_level: &mut HashMap<u64, (Side, usize)>,
        ) -> u64 {
            let mut done_q = 0;

            for o in price_level.iter_mut() {
                if o.quantity <= *incoming_order_qty {
                    *incoming_order_qty -= o.quantity;
                    done_q += o.quantity;
                    o.quantity = 0;
                    order_level.remove(&o.id);
                } else {
                    o.quantity -= *incoming_order_qty;
                    done_q += *incoming_order_qty;
                    *incoming_order_qty = 0;
                }
            }
            price_level.retain(|x| x.quantity != 0);
            done_q
        }

        let mut remaining_q = order_q;

        println!("Got order with quantity {} at price {}", remaining_q, price);

        let mut fill_result = FillResult::new();

        match s {
            Side::Bid => {
                let book = &mut self.asks;
                let price_map = &mut book.price_map;
                let price_levels = &mut book.price_levels;
                let mut price_map_iter = price_map.iter();

                if let Some((mut x, _)) = price_map_iter.next() {
                    while price >= *x {
                        let curr_level = price_map[x];
                        let matched_q = match_at_price_level(
                            &mut price_levels[curr_level],
                            &mut remaining_q,
                            &mut self.order_level,
                        );

                        if matched_q != 0 {
                            println!("Successfully matched {} quantity at level {}", matched_q, x);
                            fill_result.filled_orders.push((matched_q, *x));
                        }

                        if let Some((a, _)) = price_map_iter.next() {
                            x = a;
                        } else {
                            break;
                        }
                    }
                }
            }
            Side::Ask => {
                let book = &mut self.bids;
                let price_map = &mut book.price_map;
                let price_levels = &mut book.price_levels;
                let mut price_map_iter = price_map.iter();

                if let Some((mut x, _)) = price_map.iter().next_back() {
                    while price <= *x {
                        let curr_level = price_map[x];
                        let matched_q = match_at_price_level(
                            &mut price_levels[curr_level],
                            &mut remaining_q,
                            &mut self.order_level,
                        );

                        if matched_q != 0 {
                            println!("Successfully matched {} quantity at level {}", matched_q, x);
                            fill_result.filled_orders.push((matched_q, *x));
                        }

                        if let Some((a, _)) = price_map_iter.next_back() {
                            x = a;
                        } else {
                            break;
                        }
                    }
                }
            }
        }

        fill_result.remaining_quantity = remaining_q;

        if remaining_q != 0 {
            println!(
                "Still remaining quantity {} at price {}",
                remaining_q, price
            );

            fill_result.status = {
                if remaining_q == order_q {
                    OrderStatus::Created
                } else {
                    OrderStatus::PartiallyFilled
                }
            };
            self.create_new_limit_order(s, price, remaining_q);
        } else {
            fill_result.status = OrderStatus::Filled;
        }
        self.update_best_bid_and_ask();

        fill_result
    }
}
