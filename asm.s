// test.go
package main

func rotl(x, y uint64) uint64

func main() {
	i := 0
	var j uint64 = 1
	println(j)	
	for i < 128 {
		j = rotl(j, 1)
		println(j)	
		i++
	}	
}

// rotl_amd64.s
TEXT ·rotl(SB), $0
        MOVQ x+0(FP), BX
        MOVQ y+8(FP), CX
        ROLQ CX, BX
        MOVQ BX, ret+16(FP)
        RET
