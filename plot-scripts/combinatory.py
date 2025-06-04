
import matplotlib.pyplot as plt
from math import comb

# Function to compute the probability P(first B at position k)


def prob_first_b_at_k(N, k):
    if 1 <= k <= N+1:
        return comb(2 * N - k, N - 1) / comb(2 * N, N)
    else:
        return 0


# Plot for N from 1 to 7
plt.figure(figsize=(10, 6))

for N in range(1, 8):
    ks = list(range(1, N+2))
    probs = [prob_first_b_at_k(N, k) for k in ks]
    plt.plot(ks, probs, marker='o', label=f'N={N}')

# Plot aesthetics
plt.title('Probability of First schema change at Position k (for N from 1 to 7)')
plt.xlabel('Position k')
plt.ylabel('Probability')
plt.legend()
plt.grid(True)
plt.xticks(range(1, 7))
plt.tight_layout()
plt.savefig('probability of first schema change.png')
plt.show()
