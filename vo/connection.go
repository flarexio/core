package vo

type Connection struct {
	ConnectionCode string `json:"code"`
	ProxyID        string `json:"proxy_id"`
	Wallet         string `json:"wallet"`
	Token          string `json:"token"`
}
