export interface IceEodRow {
  trade_date: string;
  symbol: string;
  description: string | null;
  product_type: string | null;
  contract_type: string | null;
  strip: string | null;
  start_date: string | null;
  end_date: string | null;
  open: number | null;
  high: number | null;
  low: number | null;
  close: number | null;
  volume: number | null;
  vwap: number | null;
  trade_count: number | null;
  lift_count: number | null;
  hit_count: number | null;
  leg_count: number | null;
  buy_volume: number | null;
  sell_volume: number | null;
  leg_volume: number | null;
  block_trade_count: number | null;
  block_volume: number | null;
}

export interface IceTickRow {
  exec_time_local: string;
  trade_date: string;
  symbol: string;
  description: string | null;
  product_type: string | null;
  contract_type: string | null;
  strip: string | null;
  start_date: string | null;
  end_date: string | null;
  price: number | null;
  quantity: number | null;
  trade_direction: string | null;
}
