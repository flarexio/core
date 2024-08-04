package model

type ContextKey int

const (
	Config ContextKey = iota
	EdgeID
	Logger
)
