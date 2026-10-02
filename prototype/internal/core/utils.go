package core

// BoolPtr returns a pointer to the given boolean value
func BoolPtr(b bool) *bool {
	return &b
}

// SafeSubstring safely returns the first n characters of a string
func SafeSubstring(s string, n int) string {
	if len(s) >= n {
		return s[:n]
	}
	if len(s) > 0 {
		return s
	}
	return ""
}
