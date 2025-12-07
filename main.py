class Solution:
    def kthElement(self, a, b, k):
        A, B = a, b
        if len(b) < len(a):
            A, B = B, A
            
        n, m = len(A), len(B)
        low = max(0, k - m) - 1  # -1 means take 0 elements from A
        high = min(k, n) - 1

        while (low <= high):
            midA = (low + high) // 2
            midB = k - midA - 2
            
            Aleft = A[midA] if midA >= 0 else float("-inf")
            Aright = A[midA + 1] if midA + 1 < len(A) else float("inf")
            Bleft = B[midB] if midB >= 0 else float("-inf")
            Bright = B[midB + 1] if midB + 1 < len(B) else float("inf")
            
            if (Aleft <= Bright and Bleft <= Aright):
                return max(Aleft, Bleft)
            elif (Aleft > Bright):
                high = midA - 1
            else:
                low = midA + 1
        
        return -1
