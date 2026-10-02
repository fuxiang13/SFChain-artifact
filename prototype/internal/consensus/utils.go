package consensus

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
