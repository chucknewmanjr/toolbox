-- ============================================================================
declare @DecimalNumber float = 2.54
-- ============================================================================

declare @Integer int = @DecimalNumber;

declare @TargetFraction float = @DecimalNumber - @Integer;

print concat(@Integer, ' ', @TargetFraction);

declare @ThisDenominator int = 1;
declare @MaxDenominator int = 100;
declare @ThisFraction float;
declare @BastFraction float = 1;
declare @Numerator int

while @ThisDenominator < @MaxDenominator begin;
	set @Numerator = round(@ThisDenominator * @TargetFraction, 0);

	set @ThisFraction = cast(@Numerator as float) / @ThisDenominator

	if ABS(@TargetFraction - @ThisFraction) < abs(@TargetFraction - @BastFraction) begin;
		set @BastFraction = @ThisFraction;

		print concat(@Integer, ' ', @Numerator, '/', @ThisDenominator, ' (', round(abs(log10(nullif(abs(@TargetFraction - @BastFraction), 0))), 1), ')');
	end;

	set @ThisDenominator += 1;
end;


