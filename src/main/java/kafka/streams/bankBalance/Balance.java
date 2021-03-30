package kafka.streams.bankBalance;

import lombok.Builder;
import lombok.Value;

@Builder
@Value
public class Balance {
    int count;
    long balance;
    String time;
}
