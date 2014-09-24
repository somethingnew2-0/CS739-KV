package keyvalue

type Service interface {
	Get(key string) (int, string)
	Set(key string, value string) (int, string)
	Close()
}
