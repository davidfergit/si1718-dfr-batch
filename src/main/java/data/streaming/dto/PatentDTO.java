package data.streaming.dto;

import java.util.Set;

public class PatentDTO {

	private String firstPatent;
	private String secondPatent;
	private Set<String> researchers;
	private Integer rating;
	
	public PatentDTO() {
		
	}

	public PatentDTO(String firstPatent, String secondPatent, Set<String> researchers, Integer rating) {
		super();
		this.firstPatent = firstPatent;
		this.secondPatent = secondPatent;
		this.researchers = researchers;
		this.rating = rating;
	}

	public String getFirstPatent() {
		return firstPatent;
	}

	public void setFirstPatent(String firstPatent) {
		this.firstPatent = firstPatent;
	}

	public String getSecondPatent() {
		return secondPatent;
	}

	public void setSecondPatent(String secondPatent) {
		this.secondPatent = secondPatent;
	}

	public Set<String> getResearchers() {
		return researchers;
	}

	public void setResearchers(Set<String> researchers) {
		this.researchers = researchers;
	}

	public Integer getRating() {
		return rating;
	}

	public void setRating(Integer rating) {
		this.rating = rating;
	}

	@Override
	public String toString() {
		return "PatentDTO [firstPatent=" + firstPatent + ", secondPatent=" + secondPatent + ", researchers="
				+ researchers + ", rating=" + rating + "]";
	}

	
	
}
