use orderbook::{OrderBook, Side};
use rand::Rng;

fn main() {
    println!("Creating new Orderbook");
    let mut ob = OrderBook::new("Test".to_string());
    let mut rng = rand::thread_rng();
    for _ in 1..1000 {
        ob.add_limit_order(Side::Bid, rng.gen_range(1..5000), rng.gen_range(1..=500));
    }
    //dbgp!("{:#?}", ob);
    println!("Done adding orders, Starting to fill");

    for _ in 1..10 {
        for _ in 1..100 {
            ob.add_limit_order(Side::Ask, rng.gen_range(1..5000), rng.gen_range(1..=500));
        }
    }
    println!("Done!");
    ob.bbo();
}
