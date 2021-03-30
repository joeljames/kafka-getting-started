package kafka.streams.bankBalance;

import lombok.Builder;
import lombok.Value;

import java.time.OffsetDateTime;

@Value
@Builder
public class Transaction {
    String name;
    long amount;
    OffsetDateTime time;
}
